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

#[derive(Debug, Clone)]
pub struct CfAxisSummary {
    pub axis: char,
    pub dim: String,
    pub coord_var: String,
}

#[derive(Debug, Default, Clone)]
pub struct CfSummary {
    pub conventions: Option<String>,
    pub axes: Vec<CfAxisSummary>,
    pub suggested_plot_dims: Option<(String, String)>,
    pub suggested_slice_dims: Vec<String>,
    pub candidate_data_vars: Vec<String>,
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

pub fn cf_summary(metadata: &ZarrMetadata) -> CfSummary {
    let conventions = metadata
        .global_attributes
        .get("Conventions")
        .or_else(|| metadata.global_attributes.get("conventions"))
        .and_then(|v| match v {
            AttributeValue::String(s) => Some(s.clone()),
            _ => None,
        });

    let coord_vars = find_coordinate_variables(metadata);
    let coord_paths: HashSet<&str> = coord_vars.iter().map(|(p, _)| p.as_str()).collect();

    let mut bounds_paths: HashSet<String> = HashSet::new();
    for (coord_path, coord_var) in &coord_vars {
        let Some(AttributeValue::String(bounds_name)) = coord_var.attributes.get("bounds") else {
            continue;
        };

        if let Some((bounds_path, _bounds_var)) =
            resolve_related_var(metadata, coord_path, bounds_name)
        {
            bounds_paths.insert(bounds_path.clone());
        }
    }

    let mut axis_candidates: Vec<CfAxisSummary> = Vec::new();
    for (path, var) in &coord_vars {
        let axis_attr = axis_char(attr_string(var, "axis"));
        let standard_name = attr_string(var, "standard_name");
        let units = attr_string(var, "units");

        let is_time = is_time_coordinate(&var.name, axis_attr, standard_name, units);
        let is_vertical = is_vertical_coordinate(&var.name, axis_attr, standard_name);
        let is_lat = is_latitude_coordinate(standard_name, units);
        let is_lon = is_longitude_coordinate(standard_name, units);

        let axis = if is_time {
            'T'
        } else if is_vertical {
            'Z'
        } else if is_lat {
            'Y'
        } else if is_lon {
            'X'
        } else {
            axis_attr.unwrap_or('?')
        };

        let dim = var
            .dimensions
            .first()
            .map(|d| d.name.clone())
            .unwrap_or_else(|| var.name.clone());

        axis_candidates.push(CfAxisSummary {
            axis,
            dim,
            coord_var: display_var_path(path, var),
        });
    }

    let mut axes: Vec<CfAxisSummary> = Vec::new();
    for axis in ['T', 'Z', 'Y', 'X'] {
        if let Some(best) = best_axis_candidate(axis, &axis_candidates) {
            axes.push(best);
        }
    }

    // Suggested plot dims: prefer Y/X axes (lat/lon or y/x).
    let plot_dim_y = axes
        .iter()
        .find(|a| a.axis == 'Y')
        .map(|a| a.dim.clone())
        .or_else(|| find_dimension_name(metadata, &["lat", "latitude", "y"]));

    let plot_dim_x = axes
        .iter()
        .find(|a| a.axis == 'X')
        .map(|a| a.dim.clone())
        .or_else(|| find_dimension_name(metadata, &["lon", "longitude", "x"]));

    let suggested_plot_dims = match (plot_dim_y, plot_dim_x) {
        (Some(y), Some(x)) => Some((y, x)),
        _ => None,
    };

    // Suggested slice dims: prefer T and Z axes.
    let mut suggested_slice_dims: Vec<String> = Vec::new();
    if let Some(t) = axes
        .iter()
        .find(|a| a.axis == 'T')
        .map(|a| a.dim.clone())
        .or_else(|| find_dimension_name(metadata, &["time", "t"]))
    {
        suggested_slice_dims.push(t);
    }

    if let Some(z) = axes
        .iter()
        .find(|a| a.axis == 'Z')
        .map(|a| a.dim.clone())
        .or_else(|| find_dimension_name(metadata, &["lev", "level", "plev", "depth", "z"]))
    {
        if !suggested_slice_dims.iter().any(|d| d == &z) {
            suggested_slice_dims.push(z);
        }
    }

    // If plot dims are known, don't repeat them as slice dims.
    if let Some((y, x)) = &suggested_plot_dims {
        suggested_slice_dims.retain(|d| d != y && d != x);
    }

    // Candidate data variables: exclude coordinate vars, bounds vars, and grid_mapping variables.
    let mut candidates: Vec<(String, u128, usize)> = Vec::new();
    for (path, var) in &metadata.variables {
        if coord_paths.contains(path.as_str()) {
            continue;
        }

        if bounds_paths.contains(path) {
            continue;
        }

        if var.attributes.contains_key("grid_mapping_name") {
            continue;
        }

        let var_path = display_var_path(path, var);
        let nelems = approx_num_elements(&var.shape);
        candidates.push((var_path, nelems, var.shape.len()));
    }

    candidates.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| b.2.cmp(&a.2))
            .then_with(|| a.0.cmp(&b.0))
    });

    let candidate_data_vars = candidates
        .into_iter()
        .filter(|(_name, nelems, _ndim)| *nelems > 0)
        .take(10)
        .map(|(name, _nelems, _ndim)| name)
        .collect();

    CfSummary {
        conventions,
        axes,
        suggested_plot_dims,
        suggested_slice_dims,
        candidate_data_vars,
    }
}

fn approx_num_elements(shape: &[u64]) -> u128 {
    shape
        .iter()
        .copied()
        .map(u128::from)
        .fold(1u128, |acc, n| acc.saturating_mul(n))
}

fn find_dimension_name(metadata: &ZarrMetadata, preferred: &[&str]) -> Option<String> {
    for want in preferred {
        if metadata.dimensions.contains_key(*want) {
            return Some((*want).to_string());
        }
    }

    for want in preferred {
        for key in metadata.dimensions.keys() {
            if key.eq_ignore_ascii_case(want) {
                return Some(key.clone());
            }
        }
    }

    None
}

fn best_axis_candidate(axis: char, candidates: &[CfAxisSummary]) -> Option<CfAxisSummary> {
    let mut best: Option<&CfAxisSummary> = None;
    for cand in candidates.iter().filter(|c| c.axis == axis) {
        best = match best {
            None => Some(cand),
            Some(current) => {
                if axis_candidate_rank(axis, cand) < axis_candidate_rank(axis, current) {
                    Some(cand)
                } else {
                    Some(current)
                }
            }
        };
    }

    best.cloned()
}

fn axis_candidate_rank(axis: char, cand: &CfAxisSummary) -> (u8, usize, &str) {
    let pref = axis_preference(axis, &cand.dim);
    let depth = cand.coord_var.matches('/').count();
    (pref, depth, cand.coord_var.as_str())
}

fn axis_preference(axis: char, dim: &str) -> u8 {
    match axis {
        'T' => {
            if dim.eq_ignore_ascii_case("time") {
                0
            } else {
                1
            }
        }
        'Y' => {
            if dim.eq_ignore_ascii_case("lat") || dim.eq_ignore_ascii_case("latitude") {
                0
            } else if dim.eq_ignore_ascii_case("y") {
                1
            } else {
                2
            }
        }
        'X' => {
            if dim.eq_ignore_ascii_case("lon") || dim.eq_ignore_ascii_case("longitude") {
                0
            } else if dim.eq_ignore_ascii_case("x") {
                1
            } else {
                2
            }
        }
        'Z' => {
            if dim.eq_ignore_ascii_case("lev")
                || dim.eq_ignore_ascii_case("level")
                || dim.eq_ignore_ascii_case("plev")
                || dim.eq_ignore_ascii_case("depth")
            {
                0
            } else {
                1
            }
        }
        _ => 1,
    }
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
        let dim_names_attr_present = var.attributes.contains_key("_ARRAY_DIMENSIONS")
            || var.attributes.contains_key("dimension_names");

        if !dim_names_attr_present && !var.shape.is_empty() {
            report.warn(format!(
                "Variable '{}' has no explicit dimension name list (_ARRAY_DIMENSIONS/dimension_names); CF tooling may have trouble interpreting axes.",
                display_var_path(path, var)
            ));
        }

        check_one_dimension_name_attr(metadata, report, path, var, "_ARRAY_DIMENSIONS");
        check_one_dimension_name_attr(metadata, report, path, var, "dimension_names");
    }
}

fn check_one_dimension_name_attr(
    _metadata: &ZarrMetadata,
    report: &mut CfReport,
    path: &str,
    var: &Variable,
    attr_name: &str,
) {
    let Some(attr) = var.attributes.get(attr_name) else {
        return;
    };

    let AttributeValue::Array(items) = attr else {
        report.warn(format!(
            "Variable '{}' attribute '{}' is present but not an array (found {}).",
            display_var_path(path, var),
            attr_name,
            describe_attr_value(attr)
        ));
        return;
    };

    if items.len() != var.shape.len() {
        report.error(format!(
            "Variable '{}' {} length ({}) does not match shape dimensionality ({}).",
            display_var_path(path, var),
            attr_name,
            items.len(),
            var.shape.len()
        ));
    }

    let mut names: Vec<&str> = Vec::new();
    let mut any_non_string = false;
    for item in items {
        match item {
            AttributeValue::String(s) => names.push(s.as_str()),
            _ => any_non_string = true,
        }
    }

    if any_non_string {
        report.warn(format!(
            "Variable '{}' {} contains non-string entries; expected an array of strings.",
            display_var_path(path, var),
            attr_name
        ));
    }

    let mut seen: HashSet<&str> = HashSet::new();
    for name in names {
        if name.trim().is_empty() {
            report.warn(format!(
                "Variable '{}' {} contains an empty dimension name.",
                display_var_path(path, var),
                attr_name
            ));
        }

        if !seen.insert(name) {
            report.warn(format!(
                "Variable '{}' {} contains duplicate dimension name '{}'.",
                display_var_path(path, var),
                attr_name,
                name
            ));
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
        let coord_label = display_var_path(path, var);

        let axis = axis_char(attr_string(var, "axis"));
        let standard_name = attr_string(var, "standard_name");
        let units = attr_string(var, "units");

        let is_time = is_time_coordinate(dim, axis, standard_name, units);
        let is_vertical = is_vertical_coordinate(dim, axis, standard_name);
        let is_lat = is_latitude_coordinate(standard_name, units);
        let is_lon = is_longitude_coordinate(standard_name, units);

        if len == 0 {
            report.warn(format!(
                "Coordinate variable '{}' has length 0.",
                coord_label
            ));
        }

        // units
        match var.attributes.get("units") {
            Some(AttributeValue::String(_)) => {}
            Some(other) => report.warn(format!(
                "Coordinate variable '{}' has non-string 'units' attribute: {}",
                coord_label,
                describe_attr_value(other)
            )),
            None => report.warn(format!(
                "Coordinate variable '{}' is missing 'units' attribute.",
                coord_label
            )),
        }

        // standard_name (optional, but useful)
        if let Some(AttributeValue::String(sn)) = var.attributes.get("standard_name") {
            report.info(format!(
                "Coordinate variable '{}' standard_name='{}'",
                coord_label, sn
            ));
        }

        // CF-ish time coordinate checks.
        if is_time {
            if let Some(units) = units {
                if !cf_time_units_looks_ok(units) {
                    report.warn(format!(
                        "Time coordinate variable '{}' has units='{}' (expected e.g. 'days since 1850-01-01').",
                        coord_label, units
                    ));
                }
            }

            match var.attributes.get("calendar") {
                Some(AttributeValue::String(_)) | None => {}
                Some(other) => report.warn(format!(
                    "Time coordinate variable '{}' has non-string 'calendar' attribute: {}",
                    coord_label,
                    describe_attr_value(other)
                )),
            }
        }

        // CF-ish vertical coordinate checks.
        if is_vertical {
            let positive_required = vertical_positive_required(dim, standard_name);

            match var.attributes.get("positive") {
                Some(AttributeValue::String(pos)) => {
                    let pos_lc = pos.to_ascii_lowercase();
                    if pos_lc != "up" && pos_lc != "down" {
                        report.warn(format!(
                            "Vertical coordinate variable '{}' has positive='{}' (expected 'up' or 'down').",
                            coord_label, pos
                        ));
                    }
                }
                Some(other) => report.warn(format!(
                    "Vertical coordinate variable '{}' has non-string 'positive' attribute: {}",
                    coord_label,
                    describe_attr_value(other)
                )),
                None => {
                    if positive_required {
                        report.warn(format!(
                            "Vertical coordinate variable '{}' is missing 'positive' attribute (expected 'up' or 'down').",
                            coord_label
                        ));
                    }
                }
            }
        }

        // bounds
        if let Some(AttributeValue::String(bounds_name)) = var.attributes.get("bounds") {
            check_bounds_variable(metadata, path, var, bounds_name, report);
        }

        // Light-touch monotonicity and sanity checks (sample, to avoid huge reads).
        if len >= 2 {
            let sample = len.min(10_000);
            let range = 0..sample;

            let missing_values = collect_missing_values_f64(var);

            match store.read_array_subset_f64(var, std::slice::from_ref(&range)) {
                Ok(data) => {
                    let direction = monotonic_direction(&data, &missing_values);

                    if is_time {
                        match direction {
                            Some("increasing") => report.info(format!(
                                "Time coordinate '{}' appears monotonic increasing (checked first {} values).",
                                dim, sample
                            )),
                            Some("decreasing") => report.warn(format!(
                                "Time coordinate '{}' appears monotonic decreasing (expected increasing; checked first {} values).",
                                dim, sample
                            )),
                            Some("constant") => report.warn(format!(
                                "Time coordinate '{}' appears constant (expected increasing; checked first {} values).",
                                dim, sample
                            )),
                            None => report.warn(format!(
                                "Time coordinate '{}' is not monotonic (expected increasing; checked first {} values).",
                                dim, sample
                            )),
                            Some(other) => report.info(format!(
                                "Time coordinate '{}' monotonicity: {} (checked first {} values).",
                                dim, other, sample
                            )),
                        }
                    } else {
                        match direction {
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

                    if is_lat || is_lon {
                        if let Some((min, max)) = sample_min_max(&data, &missing_values) {
                            if is_lat && (min < -90.0 - 1e-6 || max > 90.0 + 1e-6) {
                                report.warn(format!(
                                    "Latitude coordinate '{}' sample range [{:.6}, {:.6}] looks out of bounds for degrees_north.",
                                    dim, min, max
                                ));
                            }

                            if is_lon && (min < -360.0 - 1e-6 || max > 360.0 + 1e-6) {
                                report.warn(format!(
                                    "Longitude coordinate '{}' sample range [{:.6}, {:.6}] looks out of bounds for degrees_east.",
                                    dim, min, max
                                ));
                            }
                        }
                    }
                }
                Err(err) => {
                    let kind = if is_time {
                        "time coordinate"
                    } else {
                        "coordinate"
                    };
                    report.warn(format!(
                        "Skipping monotonicity check for {} '{}' ({}): {}",
                        kind, dim, coord_label, err
                    ));
                }
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

fn attr_string<'a>(var: &'a Variable, key: &str) -> Option<&'a str> {
    match var.attributes.get(key) {
        Some(AttributeValue::String(s)) => Some(s.as_str()),
        _ => None,
    }
}

fn axis_char(axis: Option<&str>) -> Option<char> {
    axis.and_then(|s| s.chars().next())
        .map(|c| c.to_ascii_uppercase())
}

fn starts_with_ignore_ascii_case(s: &str, prefix: &str) -> bool {
    s.get(0..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
}

fn cf_time_units_looks_ok(units: &str) -> bool {
    let units = units.trim();
    if units.is_empty() {
        return false;
    }

    let u = units.to_ascii_lowercase();
    let Some((prefix, rest)) = u.split_once(" since ") else {
        return false;
    };

    let prefix = prefix.trim();
    let rest = rest.trim();

    if rest.is_empty() || !rest.chars().any(|c| c.is_ascii_digit()) {
        return false;
    }

    matches!(
        prefix,
        "seconds"
            | "second"
            | "minutes"
            | "minute"
            | "hours"
            | "hour"
            | "days"
            | "day"
            | "months"
            | "month"
            | "years"
            | "year"
    )
}

fn is_time_coordinate(
    name: &str,
    axis: Option<char>,
    standard_name: Option<&str>,
    units: Option<&str>,
) -> bool {
    if axis == Some('T') {
        return true;
    }

    if standard_name.is_some_and(|sn| sn.eq_ignore_ascii_case("time")) {
        return true;
    }

    if name.eq_ignore_ascii_case("time") || starts_with_ignore_ascii_case(name, "time") {
        return true;
    }

    units.is_some_and(cf_time_units_looks_ok)
}

fn units_looks_like_latitude(units: &str) -> bool {
    let u = units.to_ascii_lowercase();
    u.contains("degrees_north") || u.contains("degree_north")
}

fn units_looks_like_longitude(units: &str) -> bool {
    let u = units.to_ascii_lowercase();
    u.contains("degrees_east") || u.contains("degree_east")
}

fn is_latitude_coordinate(standard_name: Option<&str>, units: Option<&str>) -> bool {
    standard_name.is_some_and(|sn| sn.eq_ignore_ascii_case("latitude"))
        || units.is_some_and(units_looks_like_latitude)
}

fn is_longitude_coordinate(standard_name: Option<&str>, units: Option<&str>) -> bool {
    standard_name.is_some_and(|sn| sn.eq_ignore_ascii_case("longitude"))
        || units.is_some_and(units_looks_like_longitude)
}

fn standard_name_suggests_vertical(sn: &str) -> bool {
    sn.eq_ignore_ascii_case("air_pressure")
        || sn.eq_ignore_ascii_case("depth")
        || sn.eq_ignore_ascii_case("altitude")
        || sn.eq_ignore_ascii_case("geopotential_height")
        || sn.eq_ignore_ascii_case("model_level_number")
        || sn.eq_ignore_ascii_case("atmosphere_hybrid_sigma_pressure_coordinate")
        || sn.eq_ignore_ascii_case("atmosphere_hybrid_height_coordinate")
        || sn.eq_ignore_ascii_case("atmosphere_sigma_coordinate")
        || sn.eq_ignore_ascii_case("ocean_sigma_coordinate")
        || sn.eq_ignore_ascii_case("ocean_sigma_z_coordinate")
        || sn.eq_ignore_ascii_case("ocean_s_coordinate")
        || sn.eq_ignore_ascii_case("ocean_s_coordinate_g1")
        || sn.eq_ignore_ascii_case("ocean_s_coordinate_g2")
        || sn.eq_ignore_ascii_case("ocean_double_sigma_coordinate")
}

fn is_vertical_coordinate(name: &str, axis: Option<char>, standard_name: Option<&str>) -> bool {
    if axis == Some('Z') {
        return true;
    }

    if standard_name.is_some_and(standard_name_suggests_vertical) {
        return true;
    }

    name.eq_ignore_ascii_case("lev")
        || name.eq_ignore_ascii_case("level")
        || name.eq_ignore_ascii_case("plev")
        || name.eq_ignore_ascii_case("depth")
        || name.eq_ignore_ascii_case("altitude")
        || name.eq_ignore_ascii_case("height")
        || name.eq_ignore_ascii_case("z")
}

fn vertical_positive_required(name: &str, standard_name: Option<&str>) -> bool {
    if name.eq_ignore_ascii_case("depth")
        || name.eq_ignore_ascii_case("altitude")
        || name.eq_ignore_ascii_case("height")
    {
        return true;
    }

    standard_name.is_some_and(|sn| {
        sn.eq_ignore_ascii_case("depth")
            || sn.eq_ignore_ascii_case("altitude")
            || sn.eq_ignore_ascii_case("geopotential_height")
    })
}

fn sample_min_max(values: &[f64], missing_values: &[f64]) -> Option<(f64, f64)> {
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    let mut any = false;

    for v in values.iter().copied() {
        if !v.is_finite() || missing_values.contains(&v) {
            continue;
        }

        any = true;
        min = min.min(v);
        max = max.max(v);
    }

    if any { Some((min, max)) } else { None }
}

fn monotonic_direction(values: &[f64], missing_values: &[f64]) -> Option<&'static str> {
    // Ignore non-finite and missing values for monotonicity checks.
    let filtered: Vec<f64> = values
        .iter()
        .copied()
        .filter(|v| v.is_finite() && !missing_values.contains(v))
        .collect();
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

fn collect_missing_values_f64(var: &Variable) -> Vec<f64> {
    let mut out: Vec<f64> = Vec::new();

    if let Some(v) = var.attributes.get("_FillValue") {
        push_missing_values_attr(&mut out, v);
    }

    if let Some(v) = var.attributes.get("missing_value") {
        push_missing_values_attr(&mut out, v);
    }

    if let Some(v) = &var.fill_value {
        push_missing_values_attr(&mut out, v);
    }

    out.sort_by(|a, b| a.total_cmp(b));
    out.dedup();
    out
}

fn push_missing_values_attr(out: &mut Vec<f64>, value: &AttributeValue) {
    match value {
        AttributeValue::Number(v) => out.push(*v),
        AttributeValue::Integer(v) => out.push(*v as f64),
        AttributeValue::Array(values) => {
            for v in values {
                push_missing_values_attr(out, v);
            }
        }
        _ => {}
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
        assert_eq!(
            monotonic_direction(&[0.0, 1.0, 2.0], &[]),
            Some("increasing")
        );
        assert_eq!(
            monotonic_direction(&[2.0, 1.0, 0.0], &[]),
            Some("decreasing")
        );
        assert_eq!(monotonic_direction(&[1.0, 1.0, 1.0], &[]), Some("constant"));
        assert_eq!(monotonic_direction(&[0.0, 1.0, 0.5], &[]), None);

        // Non-finite values are ignored
        assert_eq!(
            monotonic_direction(&[0.0, f64::NAN, 1.0, 2.0], &[]),
            Some("increasing")
        );

        // Missing values are ignored
        assert_eq!(
            monotonic_direction(&[0.0, -9999.0, 1.0, 2.0], &[-9999.0]),
            Some("increasing")
        );
    }

    #[test]
    fn test_cf_time_units_looks_ok() {
        assert!(cf_time_units_looks_ok("days since 1850-01-01"));
        assert!(cf_time_units_looks_ok("hours since 2000-01-01 00:00:00"));
        assert!(cf_time_units_looks_ok("seconds since 1970-01-01T00:00:00Z"));

        assert!(!cf_time_units_looks_ok("days"));
        assert!(!cf_time_units_looks_ok("meters"));
        assert!(!cf_time_units_looks_ok("days since"));
        assert!(!cf_time_units_looks_ok("days since not-a-date"));
    }

    #[test]
    fn test_lat_lon_detection() {
        assert!(units_looks_like_latitude("degrees_north"));
        assert!(units_looks_like_latitude("degree_north"));
        assert!(units_looks_like_longitude("degrees_east"));
        assert!(units_looks_like_longitude("degree_east"));

        assert!(is_latitude_coordinate(Some("latitude"), None));
        assert!(is_longitude_coordinate(Some("longitude"), None));
        assert!(is_latitude_coordinate(None, Some("degrees_north")));
        assert!(is_longitude_coordinate(None, Some("degrees_east")));
    }

    #[test]
    fn test_vertical_positive_required() {
        assert!(vertical_positive_required("depth", None));
        assert!(vertical_positive_required("lev", Some("depth")));
        assert!(!vertical_positive_required("lev", Some("air_pressure")));
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
