use crate::metadata::{AttributeValue, Variable, ZarrMetadata};
use crate::store::ZarrStore;
use anyhow::Result;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Level {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone)]
struct Issue {
    level: Level,
    message: String,
}

#[derive(Debug, Default, Clone)]
pub struct CfReport {
    issues: Vec<Issue>,
    warnings: usize,
    errors: usize,
}

impl CfReport {
    fn info(&mut self, msg: impl Into<String>) {
        self.issues.push(Issue {
            level: Level::Info,
            message: msg.into(),
        });
    }

    fn warn(&mut self, msg: impl Into<String>) {
        self.warnings += 1;
        self.issues.push(Issue {
            level: Level::Warning,
            message: msg.into(),
        });
    }

    fn error(&mut self, msg: impl Into<String>) {
        self.errors += 1;
        self.issues.push(Issue {
            level: Level::Error,
            message: msg.into(),
        });
    }

    pub fn has_errors(&self) -> bool {
        self.errors > 0
    }

    pub fn print(&self) {
        println!("cf-check {{");

        for issue in &self.issues {
            let tag = match issue.level {
                Level::Info => "INFO",
                Level::Warning => "WARN",
                Level::Error => "ERROR",
            };
            println!("  {}: {}", tag, issue.message);
        }

        println!("}}");
        println!(
            "Summary: {} warnings, {} errors",
            self.warnings, self.errors
        );
    }
}

pub async fn cf_check(store: &ZarrStore, metadata: &ZarrMetadata) -> Result<CfReport> {
    let mut report = CfReport::default();

    check_global_conventions(metadata, &mut report);
    check_dimension_names(metadata, &mut report);

    let coord_vars = find_coordinate_variables(metadata);
    check_coordinate_variables(store, metadata, &coord_vars, &mut report).await?;
    check_dimensions_have_coordinates(metadata, &coord_vars, &mut report);

    check_grid_mappings(metadata, &mut report);
    check_coordinates_attribute_refs(metadata, &mut report);

    Ok(report)
}

fn check_global_conventions(metadata: &ZarrMetadata, report: &mut CfReport) {
    let conv = metadata
        .global_attributes
        .get("Conventions")
        .or_else(|| metadata.global_attributes.get("conventions"));

    match conv {
        None => report.warn("Global attribute 'Conventions' is missing (CF datasets usually set this, e.g. 'CF-1.8')."),
        Some(AttributeValue::String(s)) => {
            if s.contains("CF-") {
                report.info(format!("Conventions = '{s}'"));
            } else {
                report.warn(format!(
                    "Global attribute 'Conventions' is present but does not contain 'CF-': '{s}'"
                ));
            }
        }
        Some(other) => report.warn(format!(
            "Global attribute 'Conventions' is present but not a string: {}",
            describe_attr_value(other)
        )),
    }
}

fn check_dimension_names(metadata: &ZarrMetadata, report: &mut CfReport) {
    for (path, var) in &metadata.variables {
        let dim_names_present = var.attributes.contains_key("_ARRAY_DIMENSIONS")
            || var.attributes.contains_key("dimension_names");

        if !dim_names_present && !var.shape.is_empty() {
            report.warn(format!(
                "Variable '{}' has no explicit dimension name list (_ARRAY_DIMENSIONS/dimension_names); CF tooling may have trouble interpreting axes.",
                display_var_path(path, var)
            ));
        }

        if let Some(AttributeValue::Array(dims)) = var.attributes.get("_ARRAY_DIMENSIONS") {
            if dims.len() != var.shape.len() {
                report.error(format!(
                    "Variable '{}' _ARRAY_DIMENSIONS length ({}) does not match shape dimensionality ({}).",
                    display_var_path(path, var),
                    dims.len(),
                    var.shape.len()
                ));
            }
        }

        if let Some(AttributeValue::Array(dims)) = var.attributes.get("dimension_names") {
            if dims.len() != var.shape.len() {
                report.error(format!(
                    "Variable '{}' dimension_names length ({}) does not match shape dimensionality ({}).",
                    display_var_path(path, var),
                    dims.len(),
                    var.shape.len()
                ));
            }
        }
    }
}

fn find_coordinate_variables(metadata: &ZarrMetadata) -> Vec<(&String, &Variable)> {
    metadata
        .variables
        .iter()
        .filter(|(_path, var)| var.dimensions.len() == 1)
        .filter(|(_path, var)| {
            let dim = &var.dimensions[0].name;
            var.name == *dim
        })
        .collect()
}

async fn check_coordinate_variables(
    store: &ZarrStore,
    metadata: &ZarrMetadata,
    coord_vars: &[(&String, &Variable)],
    report: &mut CfReport,
) -> Result<()> {
    for (path, var) in coord_vars {
        let dim = &var.dimensions[0].name;
        let len = var.shape.first().copied().unwrap_or(0);

        if len == 0 {
            report.warn(format!(
                "Coordinate variable '{}' has length 0.",
                display_var_path(path, var)
            ));
        }

        // units
        match var.attributes.get("units") {
            Some(AttributeValue::String(_)) => {}
            Some(other) => report.warn(format!(
                "Coordinate variable '{}' has non-string 'units' attribute: {}",
                display_var_path(path, var),
                describe_attr_value(other)
            )),
            None => report.warn(format!(
                "Coordinate variable '{}' is missing 'units' attribute.",
                display_var_path(path, var)
            )),
        }

        // standard_name (optional, but useful)
        if let Some(AttributeValue::String(sn)) = var.attributes.get("standard_name") {
            report.info(format!(
                "Coordinate variable '{}' standard_name='{}'",
                display_var_path(path, var),
                sn
            ));
        }

        // bounds
        if let Some(AttributeValue::String(bounds_name)) = var.attributes.get("bounds") {
            check_bounds_variable(metadata, path, var, bounds_name, report);
        }

        // Light-touch monotonicity check (sample, to avoid huge reads).
        if len >= 2 {
            let sample = len.min(10_000);
            let range = 0..sample;
            let data = store.read_array_subset_f64(var, std::slice::from_ref(&range))?;
            match monotonic_direction(&data) {
                Some("increasing") => report.info(format!(
                    "Coordinate '{}' appears monotonic increasing (checked first {} values).",
                    dim, sample
                )),
                Some("decreasing") => report.info(format!(
                    "Coordinate '{}' appears monotonic decreasing (checked first {} values).",
                    dim, sample
                )),
                Some("constant") => report.warn(format!(
                    "Coordinate '{}' appears constant (checked first {} values).",
                    dim, sample
                )),
                None => report.warn(format!(
                    "Coordinate '{}' is not monotonic (checked first {} values).",
                    dim, sample
                )),
                Some(other) => report.info(format!(
                    "Coordinate '{}' monotonicity: {} (checked first {} values).",
                    dim, other, sample
                )),
            }
        }
    }

    // Also note dimensions that are used but have no coordinate variable.
    // This is handled separately by check_dimensions_have_coordinates.

    // If we have no coordinate variables at all, mention it once.
    if coord_vars.is_empty() && !metadata.dimensions.is_empty() {
        report.warn("No coordinate variables detected (1D vars named like their dimension). Many CF datasets include them for axes like time/lat/lon." );
    }

    Ok(())
}

fn check_dimensions_have_coordinates(
    metadata: &ZarrMetadata,
    coord_vars: &[(&String, &Variable)],
    report: &mut CfReport,
) {
    let mut dims_with_coord: HashSet<String> = HashSet::new();
    for (_path, var) in coord_vars {
        if let Some(dim) = var.dimensions.first() {
            dims_with_coord.insert(dim.name.clone());
        }
    }

    for (dim_name, dim_info) in &metadata.dimensions {
        // Heuristic: dims of length 2 are often bounds dims; don't require coordinate var.
        if dim_info.max_length == 2 {
            continue;
        }

        // Skip internal default dims; these usually indicate missing _ARRAY_DIMENSIONS.
        if dim_name.starts_with("dim_") {
            continue;
        }

        if !dims_with_coord.contains(dim_name) {
            report.warn(format!(
                "Dimension '{}' has no coordinate variable '{}' (1D var with same name).",
                dim_name, dim_name
            ));
        }
    }
}

fn check_bounds_variable(
    metadata: &ZarrMetadata,
    coord_path: &str,
    coord_var: &Variable,
    bounds_name: &str,
    report: &mut CfReport,
) {
    let resolved = resolve_related_var(metadata, coord_path, bounds_name);
    let Some((bounds_path, bounds_var)) = resolved else {
        report.warn(format!(
            "Coordinate '{}' declares bounds='{}' but bounds variable was not found.",
            coord_var.name, bounds_name
        ));
        return;
    };

    let coord_len = coord_var.shape.first().copied().unwrap_or(0);
    if bounds_var.shape.len() < 2 {
        report.warn(format!(
            "Bounds variable '{}' has shape {:?}; expected at least 2 dimensions (e.g. (n, 2)).",
            display_var_path(bounds_path, bounds_var),
            bounds_var.shape
        ));
        return;
    }

    if bounds_var.shape[0] != coord_len {
        report.warn(format!(
            "Bounds variable '{}' first dimension size {} does not match coordinate '{}' length {}.",
            display_var_path(bounds_path, bounds_var),
            bounds_var.shape[0],
            coord_var.name,
            coord_len
        ));
    }

    if bounds_var.shape[1] != 2 {
        report.warn(format!(
            "Bounds variable '{}' second dimension size is {} (often 2 in CF).",
            display_var_path(bounds_path, bounds_var),
            bounds_var.shape[1]
        ));
    }
}

fn check_grid_mappings(metadata: &ZarrMetadata, report: &mut CfReport) {
    for (path, var) in &metadata.variables {
        let Some(AttributeValue::String(grid_mapping)) = var.attributes.get("grid_mapping") else {
            continue;
        };

        let resolved = resolve_related_var(metadata, path, grid_mapping);
        let Some((gm_path, gm_var)) = resolved else {
            report.warn(format!(
                "Variable '{}' references grid_mapping='{}' but mapping variable was not found.",
                display_var_path(path, var),
                grid_mapping
            ));
            continue;
        };

        match gm_var.attributes.get("grid_mapping_name") {
            Some(AttributeValue::String(name)) => report.info(format!(
                "grid_mapping '{}' found (grid_mapping_name='{}') for variable '{}'.",
                display_var_path(gm_path, gm_var),
                name,
                display_var_path(path, var)
            )),
            Some(other) => report.warn(format!(
                "grid_mapping '{}' exists but grid_mapping_name is not a string: {}",
                display_var_path(gm_path, gm_var),
                describe_attr_value(other)
            )),
            None => report.warn(format!(
                "grid_mapping '{}' exists but is missing grid_mapping_name attribute.",
                display_var_path(gm_path, gm_var)
            )),
        }
    }
}

fn check_coordinates_attribute_refs(metadata: &ZarrMetadata, report: &mut CfReport) {
    for (path, var) in &metadata.variables {
        let Some(AttributeValue::String(coords)) = var.attributes.get("coordinates") else {
            continue;
        };

        for name in coords.split_whitespace() {
            if resolve_related_var(metadata, path, name).is_none() {
                report.warn(format!(
                    "Variable '{}' lists coordinates='{}' but '{}' was not found.",
                    display_var_path(path, var),
                    coords,
                    name
                ));
            }
        }
    }
}

fn resolve_related_var<'a>(
    metadata: &'a ZarrMetadata,
    source_path: &str,
    name: &str,
) -> Option<(&'a String, &'a Variable)> {
    // Try absolute-ish lookup first.
    if let Some(v) = metadata.variables.get_key_value(name) {
        return Some(v);
    }

    // Try relative to group.
    if let Some((parent, _)) = source_path.rsplit_once('/') {
        let candidate = format!("{}/{}", parent, name);
        if let Some(v) = metadata.variables.get_key_value(&candidate) {
            return Some(v);
        }
    }

    None
}

fn monotonic_direction(values: &[f64]) -> Option<&'static str> {
    // Ignore non-finite values for monotonicity checks.
    let filtered: Vec<f64> = values.iter().copied().filter(|v| v.is_finite()).collect();
    if filtered.len() < 2 {
        return None;
    }

    let mut nondecreasing = true;
    let mut nonincreasing = true;
    let mut any_change = false;

    for w in filtered.windows(2) {
        let a = w[0];
        let b = w[1];
        if b < a {
            nondecreasing = false;
        }
        if b > a {
            nonincreasing = false;
        }
        if (b - a) != 0.0 {
            any_change = true;
        }
        if !nondecreasing && !nonincreasing {
            return None;
        }
    }

    if !any_change {
        return Some("constant");
    }

    if nondecreasing {
        Some("increasing")
    } else if nonincreasing {
        Some("decreasing")
    } else {
        None
    }
}

fn display_var_path(path: &str, var: &Variable) -> String {
    if path.is_empty() {
        "root".to_string()
    } else {
        // Prefer full path for clarity.
        var.path.clone()
    }
}

fn describe_attr_value(value: &AttributeValue) -> String {
    match value {
        AttributeValue::String(_) => "string".to_string(),
        AttributeValue::Number(_) => "number".to_string(),
        AttributeValue::Integer(_) => "integer".to_string(),
        AttributeValue::Boolean(_) => "boolean".to_string(),
        AttributeValue::Array(_) => "array".to_string(),
        AttributeValue::Object(_) => "object".to_string(),
        AttributeValue::Null => "null".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_monotonic_direction() {
        assert_eq!(monotonic_direction(&[0.0, 1.0, 2.0]), Some("increasing"));
        assert_eq!(monotonic_direction(&[2.0, 1.0, 0.0]), Some("decreasing"));
        assert_eq!(monotonic_direction(&[1.0, 1.0, 1.0]), Some("constant"));
        assert_eq!(monotonic_direction(&[0.0, 1.0, 0.5]), None);

        // Non-finite values are ignored
        assert_eq!(
            monotonic_direction(&[0.0, f64::NAN, 1.0, 2.0]),
            Some("increasing")
        );
    }

    #[test]
    fn test_resolve_related_var_root_and_group() {
        let mut md = ZarrMetadata::new();

        let v_root = Variable {
            name: "time".to_string(),
            path: "time".to_string(),
            dtype: "<f8".to_string(),
            shape: vec![10],
            chunks: vec![10],
            compressor: None,
            fill_value: None,
            order: "C".to_string(),
            filters: vec![],
            attributes: HashMap::new(),
            dimensions: vec![],
        };
        md.variables.insert("time".to_string(), v_root);

        let v_group = Variable {
            name: "lat".to_string(),
            path: "grp/lat".to_string(),
            dtype: "<f8".to_string(),
            shape: vec![10],
            chunks: vec![10],
            compressor: None,
            fill_value: None,
            order: "C".to_string(),
            filters: vec![],
            attributes: HashMap::new(),
            dimensions: vec![],
        };
        md.variables.insert("grp/lat".to_string(), v_group);

        assert!(resolve_related_var(&md, "temp", "time").is_some());
        assert!(resolve_related_var(&md, "grp/temp", "lat").is_some());
        assert!(resolve_related_var(&md, "grp/temp", "missing").is_none());
    }
}
