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
    pub dims: Vec<String>,
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
    check_ancillary_variables_attribute_refs(metadata, &mut report);
    check_cell_measures_attribute(metadata, &mut report);
    check_cell_methods_attribute(metadata, &mut report);
    check_parametric_vertical_coordinates(metadata, &mut report);

    check_lat_lon_variables(store, metadata, &mut report);

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
    let coord_var_paths: HashSet<String> = coord_vars.iter().map(|(p, _)| (*p).clone()).collect();

    let coord_refs = collect_coordinate_references(metadata);
    let ancillary_refs = collect_whitespace_separated_references(metadata, "ancillary_variables");
    let cell_measures_refs = collect_cell_measures_references(metadata);
    let bounds_refs = collect_bounds_references(metadata);

    let axis_candidates = collect_axis_candidates(metadata, &coord_refs);

    let time_axis = select_best_time_axis(&axis_candidates);
    let vertical_axis = select_best_vertical_axis(&axis_candidates);
    let (y_axis, x_axis, plot_dims_from_axes) = select_best_horizontal_axes(&axis_candidates);

    let mut axes: Vec<CfAxisSummary> = Vec::new();
    if let Some(c) = &time_axis {
        axes.push(axis_candidate_to_summary(c));
    }
    if let Some(c) = &vertical_axis {
        axes.push(axis_candidate_to_summary(c));
    }
    if let Some(c) = &y_axis {
        axes.push(axis_candidate_to_summary(c));
    }
    if let Some(c) = &x_axis {
        axes.push(axis_candidate_to_summary(c));
    }

    let suggested_plot_dims = plot_dims_from_axes.or_else(|| {
        let y = find_dimension_name(metadata, &["lat", "latitude", "y"])?;
        let x = find_dimension_name(metadata, &["lon", "longitude", "x"])?;
        Some((y, x))
    });

    // Suggested slice dims: prefer T and Z axes.
    let mut suggested_slice_dims: Vec<String> = Vec::new();

    if let Some(c) = &time_axis {
        if let Some(d) = c.dims.first() {
            suggested_slice_dims.push(d.clone());
        }
    } else if let Some(t) = find_dimension_name(metadata, &["time", "t"]) {
        suggested_slice_dims.push(t);
    }

    if let Some(c) = &vertical_axis {
        if let Some(d) = c.dims.first() {
            if !suggested_slice_dims.iter().any(|x| x == d) {
                suggested_slice_dims.push(d.clone());
            }
        }
    } else if let Some(z) = find_dimension_name(metadata, &["lev", "level", "plev", "depth", "z"]) {
        if !suggested_slice_dims.iter().any(|d| d == &z) {
            suggested_slice_dims.push(z);
        }
    }

    // If plot dims are known, don't repeat them as slice dims.
    if let Some((y, x)) = &suggested_plot_dims {
        suggested_slice_dims.retain(|d| d != y && d != x);
    }

    let mut exclude: HashSet<String> = HashSet::new();

    // Exclude coordinate-like variables from the candidate data variable list.
    exclude.extend(coord_var_paths);
    exclude.extend(coord_refs.clone());
    exclude.extend(ancillary_refs);
    exclude.extend(cell_measures_refs);
    exclude.extend(bounds_refs);

    for (path, var) in &metadata.variables {
        if var.attributes.contains_key("grid_mapping_name") {
            exclude.insert(path.clone());
        }
    }

    for cand in &axis_candidates {
        exclude.insert(cand.key.clone());
    }

    // Candidate data variables: sort by size (elements) and dimensionality.
    let mut candidates: Vec<(String, u128, usize)> = Vec::new();
    for (path, var) in &metadata.variables {
        if exclude.contains(path) {
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

#[derive(Debug, Clone)]
struct AxisCandidate {
    key: String,
    display: String,
    axis: char,
    dims: Vec<String>,
    is_dimension_coordinate: bool,
    is_referenced: bool,
    is_latitude: bool,
    is_longitude: bool,
    has_standard_name: bool,
    has_units: bool,
}

fn axis_candidate_to_summary(c: &AxisCandidate) -> CfAxisSummary {
    CfAxisSummary {
        axis: c.axis,
        dims: c.dims.clone(),
        coord_var: c.display.clone(),
    }
}

fn collect_axis_candidates(
    metadata: &ZarrMetadata,
    coord_refs: &HashSet<String>,
) -> Vec<AxisCandidate> {
    let mut out = Vec::new();

    for (key, var) in &metadata.variables {
        if var.dimensions.is_empty() {
            continue;
        }

        let axis_attr = axis_char(attr_string(var, "axis"));
        let standard_name = attr_string(var, "standard_name");
        let units = attr_string(var, "units");

        let is_time = is_time_coordinate(&var.name, axis_attr, standard_name, units);
        let is_vertical = is_vertical_coordinate(&var.name, axis_attr, standard_name);
        let is_latitude = is_latitude_coordinate(standard_name, units);
        let is_longitude = is_longitude_coordinate(standard_name, units);

        let axis = if is_time {
            'T'
        } else if is_vertical {
            'Z'
        } else if is_latitude {
            'Y'
        } else if is_longitude {
            'X'
        } else if standard_name.is_some_and(|sn| sn.eq_ignore_ascii_case("projection_y_coordinate"))
        {
            'Y'
        } else if standard_name.is_some_and(|sn| sn.eq_ignore_ascii_case("projection_x_coordinate"))
        {
            'X'
        } else if axis_attr == Some('Y') {
            'Y'
        } else if axis_attr == Some('X') {
            'X'
        } else {
            continue;
        };

        let dims: Vec<String> = var.dimensions.iter().map(|d| d.name.clone()).collect();

        // For summary purposes we only consider typical coordinate-like shapes:
        // - T/Z: 1D
        // - X/Y: 1D or 2D (curvilinear)
        match axis {
            'T' | 'Z' => {
                if dims.len() != 1 {
                    continue;
                }
            }
            'X' | 'Y' => {
                if dims.is_empty() || dims.len() > 2 {
                    continue;
                }
            }
            _ => continue,
        }

        let is_dimension_coordinate = dims.len() == 1 && var.name == dims[0];
        let is_referenced = coord_refs.contains(key);

        out.push(AxisCandidate {
            key: key.clone(),
            display: display_var_path(key, var),
            axis,
            dims,
            is_dimension_coordinate,
            is_referenced,
            is_latitude,
            is_longitude,
            has_standard_name: standard_name.is_some(),
            has_units: units.is_some(),
        });
    }

    out
}

fn select_best_time_axis(candidates: &[AxisCandidate]) -> Option<AxisCandidate> {
    candidates
        .iter()
        .filter(|c| c.axis == 'T' && c.dims.len() == 1)
        .min_by(|a, b| {
            rank_time_axis(a)
                .cmp(&rank_time_axis(b))
                .then_with(|| a.display.cmp(&b.display))
        })
        .cloned()
}

fn rank_time_axis(c: &AxisCandidate) -> (u8, u8, u8, u8, u8, usize) {
    let kind = u8::from(!c.is_dimension_coordinate);
    let dim_pref = if c
        .dims
        .first()
        .is_some_and(|d| d.eq_ignore_ascii_case("time"))
    {
        0
    } else {
        1
    };
    let sn = u8::from(!c.has_standard_name);
    let units = u8::from(!c.has_units);
    let referenced = u8::from(!c.is_referenced);
    let depth = c.key.matches('/').count();
    (kind, dim_pref, sn, units, referenced, depth)
}

fn select_best_vertical_axis(candidates: &[AxisCandidate]) -> Option<AxisCandidate> {
    candidates
        .iter()
        .filter(|c| c.axis == 'Z' && c.dims.len() == 1)
        .min_by(|a, b| {
            rank_vertical_axis(a)
                .cmp(&rank_vertical_axis(b))
                .then_with(|| a.display.cmp(&b.display))
        })
        .cloned()
}

fn rank_vertical_axis(c: &AxisCandidate) -> (u8, u8, u8, u8, usize) {
    let kind = u8::from(!c.is_dimension_coordinate);
    let dim_pref = if c.dims.first().is_some_and(|d| {
        d.eq_ignore_ascii_case("lev")
            || d.eq_ignore_ascii_case("level")
            || d.eq_ignore_ascii_case("plev")
            || d.eq_ignore_ascii_case("depth")
    }) {
        0
    } else {
        1
    };
    let sn = u8::from(!c.has_standard_name);
    let referenced = u8::from(!c.is_referenced);
    let depth = c.key.matches('/').count();
    (kind, dim_pref, sn, referenced, depth)
}

fn select_best_horizontal_axes(
    candidates: &[AxisCandidate],
) -> (
    Option<AxisCandidate>,
    Option<AxisCandidate>,
    Option<(String, String)>,
) {
    // Prefer a matched 2D latitude/longitude pair with shared (y,x) dims.
    let mut best_pair: Option<(&AxisCandidate, &AxisCandidate)> = None;

    for y in candidates
        .iter()
        .filter(|c| c.axis == 'Y' && c.dims.len() == 2 && c.is_latitude)
    {
        for x in candidates
            .iter()
            .filter(|c| c.axis == 'X' && c.dims.len() == 2 && c.is_longitude)
        {
            if y.dims != x.dims {
                continue;
            }

            best_pair = match best_pair {
                None => Some((y, x)),
                Some((by, bx)) => {
                    if rank_lat_lon_pair(y, x) < rank_lat_lon_pair(by, bx) {
                        Some((y, x))
                    } else {
                        Some((by, bx))
                    }
                }
            };
        }
    }

    if let Some((y, x)) = best_pair {
        let dim_y = y.dims[0].clone();
        let dim_x = y.dims[1].clone();

        return (Some((*y).clone()), Some((*x).clone()), Some((dim_y, dim_x)));
    }

    // Otherwise, pick the best 1D Y and 1D X candidates.
    let best_y = candidates
        .iter()
        .filter(|c| c.axis == 'Y' && c.dims.len() == 1)
        .min_by(|a, b| {
            rank_horizontal_1d('Y', a)
                .cmp(&rank_horizontal_1d('Y', b))
                .then_with(|| a.display.cmp(&b.display))
        })
        .cloned();

    let best_x = candidates
        .iter()
        .filter(|c| c.axis == 'X' && c.dims.len() == 1)
        .min_by(|a, b| {
            rank_horizontal_1d('X', a)
                .cmp(&rank_horizontal_1d('X', b))
                .then_with(|| a.display.cmp(&b.display))
        })
        .cloned();

    let plot_dims = match (&best_y, &best_x) {
        (Some(y), Some(x)) => Some((y.dims[0].clone(), x.dims[0].clone())),
        _ => None,
    };

    (best_y, best_x, plot_dims)
}

fn rank_lat_lon_pair<'a>(
    lat: &'a AxisCandidate,
    lon: &'a AxisCandidate,
) -> (u8, usize, usize, &'a str, &'a str) {
    let referenced = u8::from(!(lat.is_referenced && lon.is_referenced));
    let depth = lat.key.matches('/').count() + lon.key.matches('/').count();
    let dimy = lat.dims.first().map(String::as_str).unwrap_or("");
    let dimx = lat.dims.get(1).map(String::as_str).unwrap_or("");
    (
        referenced,
        depth,
        lat.display.len() + lon.display.len(),
        dimy,
        dimx,
    )
}

fn rank_horizontal_1d(axis: char, c: &AxisCandidate) -> (u8, u8, u8, u8, u8, usize) {
    let kind = u8::from(!c.is_dimension_coordinate);

    let dim_pref = match axis {
        'Y' => {
            let d = c.dims.first().map(String::as_str).unwrap_or("");
            if d.eq_ignore_ascii_case("lat") || d.eq_ignore_ascii_case("latitude") {
                0
            } else if d.eq_ignore_ascii_case("y") {
                1
            } else {
                2
            }
        }
        'X' => {
            let d = c.dims.first().map(String::as_str).unwrap_or("");
            if d.eq_ignore_ascii_case("lon") || d.eq_ignore_ascii_case("longitude") {
                0
            } else if d.eq_ignore_ascii_case("x") {
                1
            } else {
                2
            }
        }
        _ => 2,
    };

    let sn = u8::from(!c.has_standard_name);
    let units = u8::from(!c.has_units);
    let referenced = u8::from(!c.is_referenced);
    let depth = c.key.matches('/').count();

    (kind, dim_pref, sn, units, referenced, depth)
}

fn collect_whitespace_separated_references(
    metadata: &ZarrMetadata,
    attr_name: &str,
) -> HashSet<String> {
    let mut out: HashSet<String> = HashSet::new();

    for (path, var) in &metadata.variables {
        let Some(AttributeValue::String(raw)) = var.attributes.get(attr_name) else {
            continue;
        };

        for name in raw.split_whitespace() {
            if let Some((ref_path, _ref_var)) = resolve_related_var(metadata, path, name) {
                out.insert(ref_path.clone());
            }
        }
    }

    out
}

fn collect_cell_measures_references(metadata: &ZarrMetadata) -> HashSet<String> {
    let mut out: HashSet<String> = HashSet::new();

    for (path, var) in &metadata.variables {
        let Some(AttributeValue::String(raw)) = var.attributes.get("cell_measures") else {
            continue;
        };

        let (pairs, _warnings) = parse_colon_pairs(raw);
        for (_measure, name) in pairs {
            if let Some((ref_path, _ref_var)) = resolve_related_var(metadata, path, &name) {
                out.insert(ref_path.clone());
            }
        }
    }

    out
}

fn collect_bounds_references(metadata: &ZarrMetadata) -> HashSet<String> {
    let mut out: HashSet<String> = HashSet::new();

    for (path, var) in &metadata.variables {
        let Some(AttributeValue::String(bounds_name)) = var.attributes.get("bounds") else {
            continue;
        };

        if let Some((bounds_path, _bounds_var)) = resolve_related_var(metadata, path, bounds_name) {
            out.insert(bounds_path.clone());
        }
    }

    out
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
                Some(AttributeValue::String(cal)) => {
                    if !is_known_cf_calendar(cal) {
                        report.warn(format!(
                            "Time coordinate variable '{}' has unrecognized calendar='{}' (expected e.g. 'standard', 'gregorian', 'proleptic_gregorian', 'noleap', '360_day').",
                            coord_label, cal
                        ));
                    }
                }
                None => {}
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

    // Coordinate variables (strict CF coordinate vars: 1D, name == dim name)
    for (_path, var) in coord_vars {
        if let Some(dim) = var.dimensions.first() {
            dims_with_coord.insert(dim.name.clone());
        }
    }

    // Auxiliary coordinates referenced via `coordinates` also provide coordinate values for their dims
    // (e.g. curvilinear lat(y,x)/lon(y,x) for y/x).
    for coord_path in collect_coordinate_references(metadata) {
        let Some(coord_var) = metadata.variables.get(&coord_path) else {
            continue;
        };

        for d in &coord_var.dimensions {
            dims_with_coord.insert(d.name.clone());
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
        let Some(attr) = var.attributes.get("coordinates") else {
            continue;
        };

        let coords = match attr {
            AttributeValue::String(s) => s.trim(),
            other => {
                report.warn(format!(
                    "Variable '{}' has non-string 'coordinates' attribute: {}",
                    display_var_path(path, var),
                    describe_attr_value(other)
                ));
                continue;
            }
        };

        if coords.is_empty() {
            report.warn(format!(
                "Variable '{}' has an empty 'coordinates' attribute.",
                display_var_path(path, var)
            ));
            continue;
        }

        let mut seen: HashSet<&str> = HashSet::new();
        for name in coords.split_whitespace() {
            if !seen.insert(name) {
                report.warn(format!(
                    "Variable '{}' lists coordinate '{}' multiple times in coordinates='{}'.",
                    display_var_path(path, var),
                    name,
                    coords
                ));
            }

            let Some((coord_path, coord_var)) = resolve_related_var(metadata, path, name) else {
                report.warn(format!(
                    "Variable '{}' lists coordinates='{}' but '{}' was not found.",
                    display_var_path(path, var),
                    coords,
                    name
                ));
                continue;
            };

            check_var_dims_compatible(report, "coordinates", path, var, coord_path, coord_var);
        }
    }
}

fn check_ancillary_variables_attribute_refs(metadata: &ZarrMetadata, report: &mut CfReport) {
    for (path, var) in &metadata.variables {
        let Some(attr) = var.attributes.get("ancillary_variables") else {
            continue;
        };

        let vars = match attr {
            AttributeValue::String(s) => s.trim(),
            other => {
                report.warn(format!(
                    "Variable '{}' has non-string 'ancillary_variables' attribute: {}",
                    display_var_path(path, var),
                    describe_attr_value(other)
                ));
                continue;
            }
        };

        if vars.is_empty() {
            report.warn(format!(
                "Variable '{}' has an empty 'ancillary_variables' attribute.",
                display_var_path(path, var)
            ));
            continue;
        }

        let mut seen: HashSet<&str> = HashSet::new();
        for name in vars.split_whitespace() {
            if !seen.insert(name) {
                report.warn(format!(
                    "Variable '{}' lists ancillary variable '{}' multiple times in ancillary_variables='{}'.",
                    display_var_path(path, var),
                    name,
                    vars
                ));
            }

            let Some((anc_path, anc_var)) = resolve_related_var(metadata, path, name) else {
                report.warn(format!(
                    "Variable '{}' lists ancillary_variables='{}' but '{}' was not found.",
                    display_var_path(path, var),
                    vars,
                    name
                ));
                continue;
            };

            check_var_dims_compatible(report, "ancillary_variables", path, var, anc_path, anc_var);
        }
    }
}

fn check_cell_measures_attribute(metadata: &ZarrMetadata, report: &mut CfReport) {
    for (path, var) in &metadata.variables {
        let Some(attr) = var.attributes.get("cell_measures") else {
            continue;
        };

        let raw = match attr {
            AttributeValue::String(s) => s.trim(),
            other => {
                report.warn(format!(
                    "Variable '{}' has non-string 'cell_measures' attribute: {}",
                    display_var_path(path, var),
                    describe_attr_value(other)
                ));
                continue;
            }
        };

        if raw.is_empty() {
            report.warn(format!(
                "Variable '{}' has an empty 'cell_measures' attribute.",
                display_var_path(path, var)
            ));
            continue;
        }

        let (pairs, warnings) = parse_colon_pairs(raw);
        for w in warnings {
            report.warn(format!(
                "Variable '{}' has cell_measures='{}': {}",
                display_var_path(path, var),
                raw,
                w
            ));
        }

        if pairs.is_empty() {
            report.warn(format!(
                "Variable '{}' has cell_measures='{}' but no (measure: variable) pairs could be parsed.",
                display_var_path(path, var),
                raw
            ));
            continue;
        }

        for (measure, name) in pairs {
            let Some((m_path, m_var)) = resolve_related_var(metadata, path, &name) else {
                report.warn(format!(
                    "Variable '{}' has cell_measures='{}' but referenced measure var '{}' (for '{}') was not found.",
                    display_var_path(path, var),
                    raw,
                    name,
                    measure
                ));
                continue;
            };

            check_var_dims_compatible(report, "cell_measures", path, var, m_path, m_var);
        }
    }
}

fn check_cell_methods_attribute(metadata: &ZarrMetadata, report: &mut CfReport) {
    for (path, var) in &metadata.variables {
        let Some(attr) = var.attributes.get("cell_methods") else {
            continue;
        };

        let raw = match attr {
            AttributeValue::String(s) => s.trim(),
            other => {
                report.warn(format!(
                    "Variable '{}' has non-string 'cell_methods' attribute: {}",
                    display_var_path(path, var),
                    describe_attr_value(other)
                ));
                continue;
            }
        };

        if raw.is_empty() {
            report.warn(format!(
                "Variable '{}' has an empty 'cell_methods' attribute.",
                display_var_path(path, var)
            ));
            continue;
        }

        for axis in extract_cell_methods_axes(raw) {
            if axis.eq_ignore_ascii_case("interval") || axis.eq_ignore_ascii_case("comment") {
                continue;
            }

            if axis.eq_ignore_ascii_case("area") || axis.eq_ignore_ascii_case("volume") {
                continue;
            }

            let axis_is_dim = var
                .dimensions
                .iter()
                .any(|d| d.name.eq_ignore_ascii_case(&axis))
                || metadata
                    .dimensions
                    .keys()
                    .any(|k| k.eq_ignore_ascii_case(&axis));

            if !axis_is_dim {
                report.warn(format!(
                    "Variable '{}' has cell_methods='{}' but '{}' is not a known dimension name.",
                    display_var_path(path, var),
                    raw,
                    axis
                ));
            }
        }
    }
}

fn check_parametric_vertical_coordinates(metadata: &ZarrMetadata, report: &mut CfReport) {
    for (path, var) in &metadata.variables {
        let Some(AttributeValue::String(sn)) = var.attributes.get("standard_name") else {
            continue;
        };

        if !is_parametric_vertical_standard_name(sn) {
            continue;
        }

        match var.attributes.get("formula_terms") {
            Some(AttributeValue::String(ft)) => {
                let ft = ft.trim();
                if ft.is_empty() {
                    report.warn(format!(
                        "Parametric vertical coordinate variable '{}' (standard_name='{}') has empty formula_terms.",
                        display_var_path(path, var),
                        sn
                    ));
                    continue;
                }

                let (pairs, warnings) = parse_colon_pairs(ft);
                for w in warnings {
                    report.warn(format!(
                        "Parametric vertical coordinate variable '{}' formula_terms='{}': {}",
                        display_var_path(path, var),
                        ft,
                        w
                    ));
                }

                if pairs.is_empty() {
                    report.warn(format!(
                        "Parametric vertical coordinate variable '{}' formula_terms='{}' but no (term: variable) pairs could be parsed.",
                        display_var_path(path, var),
                        ft
                    ));
                    continue;
                }

                for (_term, name) in pairs {
                    if resolve_related_var(metadata, path, &name).is_none() {
                        report.warn(format!(
                            "Parametric vertical coordinate variable '{}' formula_terms references '{}' which was not found.",
                            display_var_path(path, var),
                            name
                        ));
                    }
                }
            }
            Some(other) => report.warn(format!(
                "Parametric vertical coordinate variable '{}' (standard_name='{}') has non-string formula_terms: {}",
                display_var_path(path, var),
                sn,
                describe_attr_value(other)
            )),
            None => report.warn(format!(
                "Parametric vertical coordinate variable '{}' (standard_name='{}') is missing formula_terms.",
                display_var_path(path, var),
                sn
            )),
        }
    }
}

fn check_lat_lon_variables(store: &ZarrStore, metadata: &ZarrMetadata, report: &mut CfReport) {
    let coord_vars = find_coordinate_variables(metadata);
    let coord_paths: HashSet<&str> = coord_vars.iter().map(|(p, _)| p.as_str()).collect();

    let referenced: HashSet<String> = collect_coordinate_references(metadata);

    for (path, var) in &metadata.variables {
        let standard_name = attr_string(var, "standard_name");
        let units = attr_string(var, "units");

        let is_lat = is_latitude_coordinate(standard_name, units);
        let is_lon = is_longitude_coordinate(standard_name, units);
        if !is_lat && !is_lon {
            continue;
        }

        // Avoid duplicating the 1D dimension-coordinate checks.
        if coord_paths.contains(path.as_str()) && var.dimensions.len() == 1 {
            continue;
        }

        let kind = if is_lat { "Latitude" } else { "Longitude" };
        let label = display_var_path(path, var);

        match var.attributes.get("units") {
            Some(AttributeValue::String(_)) => {}
            Some(other) => report.warn(format!(
                "{} variable '{}' has non-string 'units' attribute: {}",
                kind,
                label,
                describe_attr_value(other)
            )),
            None => report.warn(format!(
                "{} variable '{}' is missing 'units' attribute.",
                kind, label
            )),
        }

        if var.shape.is_empty() {
            continue;
        }

        let missing_values = collect_missing_values_f64(var);
        let ranges = sample_ranges_for_lat_lon(var);
        if ranges.is_empty() {
            continue;
        }

        match store.read_array_subset_f64(var, &ranges) {
            Ok(data) => {
                if let Some((min, max)) = sample_min_max(&data, &missing_values) {
                    if is_lat && (min < -90.0 - 1e-6 || max > 90.0 + 1e-6) {
                        report.warn(format!(
                            "Latitude variable '{}' sample range [{:.6}, {:.6}] looks out of bounds for degrees_north.",
                            label, min, max
                        ));
                    }

                    if is_lon && (min < -360.0 - 1e-6 || max > 360.0 + 1e-6) {
                        report.warn(format!(
                            "Longitude variable '{}' sample range [{:.6}, {:.6}] looks out of bounds for degrees_east.",
                            label, min, max
                        ));
                    }
                }
            }
            Err(err) => report.warn(format!(
                "Skipping {} range check for '{}' ({}): {}",
                kind, var.name, label, err
            )),
        }

        if var.dimensions.len() >= 2 && !referenced.contains(path) {
            report.warn(format!(
                "{} variable '{}' is not referenced by any variable via the 'coordinates' attribute.",
                kind,
                label
            ));
        }
    }
}

fn check_var_dims_compatible(
    report: &mut CfReport,
    attr_name: &str,
    parent_path: &str,
    parent: &Variable,
    child_path: &str,
    child: &Variable,
) {
    // Dimension compatibility: referenced var dims should be a subset of the parent dims.
    let mut missing: Vec<String> = Vec::new();
    for d in &child.dimensions {
        if find_dim_size(parent, &d.name).is_none() {
            missing.push(d.name.clone());
        }
    }

    if !missing.is_empty() {
        report.warn(format!(
            "Variable '{}' attribute '{}' references '{}' but referenced var dims ({}) are not a subset of variable dims ({}).",
            display_var_path(parent_path, parent),
            attr_name,
            display_var_path(child_path, child),
            missing.join(", "),
            parent
                .dimensions
                .iter()
                .map(|d| d.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
        return;
    }

    // Size compatibility for shared dims.
    for d in &child.dimensions {
        let Some((_parent_dim, parent_size)) = find_dim_size(parent, &d.name) else {
            continue;
        };

        if d.size > 0 && parent_size > 0 && d.size != parent_size {
            report.warn(format!(
                "Referenced variable '{}' dim '{}' has size {} but variable '{}' dim '{}' has size {}.",
                display_var_path(child_path, child),
                d.name,
                d.size,
                display_var_path(parent_path, parent),
                d.name,
                parent_size
            ));
        }
    }
}

fn collect_coordinate_references(metadata: &ZarrMetadata) -> HashSet<String> {
    let mut out: HashSet<String> = HashSet::new();

    for (path, var) in &metadata.variables {
        let Some(AttributeValue::String(coords)) = var.attributes.get("coordinates") else {
            continue;
        };

        for name in coords.split_whitespace() {
            if let Some((coord_path, _coord_var)) = resolve_related_var(metadata, path, name) {
                out.insert(coord_path.clone());
            }
        }
    }

    out
}

fn sample_ranges_for_lat_lon(var: &Variable) -> Vec<std::ops::Range<u64>> {
    let ndim = var.shape.len();
    if ndim == 0 {
        return Vec::new();
    }

    let limit_1d: u64 = 10_000;
    let limit_2d: u64 = 100;

    let mut ranges = Vec::with_capacity(ndim);
    for (i, &n) in var.shape.iter().enumerate() {
        let limit = if ndim == 1 {
            limit_1d
        } else if ndim == 2 {
            limit_2d
        } else if i + 2 < ndim {
            1
        } else {
            limit_2d
        };

        let end = n.min(limit);
        if end == 0 {
            return Vec::new();
        }

        ranges.push(0..end);
    }

    ranges
}

fn is_parametric_vertical_standard_name(standard_name: &str) -> bool {
    standard_name.eq_ignore_ascii_case("atmosphere_hybrid_sigma_pressure_coordinate")
        || standard_name.eq_ignore_ascii_case("atmosphere_hybrid_height_coordinate")
        || standard_name.eq_ignore_ascii_case("atmosphere_sigma_coordinate")
        || standard_name.eq_ignore_ascii_case("ocean_sigma_coordinate")
        || standard_name.eq_ignore_ascii_case("ocean_sigma_z_coordinate")
        || standard_name.eq_ignore_ascii_case("ocean_s_coordinate")
        || standard_name.eq_ignore_ascii_case("ocean_s_coordinate_g1")
        || standard_name.eq_ignore_ascii_case("ocean_s_coordinate_g2")
        || standard_name.eq_ignore_ascii_case("ocean_double_sigma_coordinate")
}

fn parse_colon_pairs(s: &str) -> (Vec<(String, String)>, Vec<String>) {
    let mut out: Vec<(String, String)> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();

    let mut pending_key: Option<String> = None;

    for token in s.split_whitespace() {
        if let Some(key) = pending_key.take() {
            out.push((key, token.to_string()));
            continue;
        }

        let Some((k, v)) = token.split_once(':') else {
            warnings.push(format!(
                "Unexpected token '{}' (expected '<key>:' or '<key>:<value>')",
                token
            ));
            continue;
        };

        let key = k.trim();
        let value = v.trim();

        if key.is_empty() {
            warnings.push(format!("Empty key in token '{}'", token));
            continue;
        }

        if value.is_empty() {
            pending_key = Some(key.to_string());
            continue;
        }

        out.push((key.to_string(), value.to_string()));
    }

    if let Some(key) = pending_key {
        warnings.push(format!("Dangling key '{}' with no value", key));
    }

    (out, warnings)
}

fn extract_cell_methods_axes(cell_methods: &str) -> HashSet<String> {
    let mut out: HashSet<String> = HashSet::new();

    for token in cell_methods.split_whitespace() {
        let clean = token.trim_matches(|c: char| matches!(c, '(' | ')' | ',' | ';'));

        if let Some(axis) = clean.strip_suffix(':') {
            if !axis.is_empty() {
                out.insert(axis.to_string());
            }
            continue;
        }

        if let Some((axis, rest)) = clean.split_once(':') {
            if !axis.is_empty() && !rest.is_empty() {
                out.insert(axis.to_string());
            }
        }
    }

    out
}

fn find_dim_size<'a>(var: &'a Variable, dim_name: &str) -> Option<(&'a str, u64)> {
    for d in &var.dimensions {
        if d.name == dim_name {
            return Some((d.name.as_str(), d.size));
        }
    }

    for d in &var.dimensions {
        if d.name.eq_ignore_ascii_case(dim_name) {
            return Some((d.name.as_str(), d.size));
        }
    }

    None
}

fn is_known_cf_calendar(calendar: &str) -> bool {
    let cal = calendar.trim().to_ascii_lowercase();
    matches!(
        cal.as_str(),
        "standard"
            | "gregorian"
            | "proleptic_gregorian"
            | "julian"
            | "noleap"
            | "365_day"
            | "all_leap"
            | "366_day"
            | "360_day"
    )
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

    fn make_var(name: &str, shape: Vec<u64>) -> Variable {
        Variable {
            name: name.to_string(),
            path: name.to_string(),
            dtype: "<f8".to_string(),
            shape: shape.clone(),
            chunks: shape,
            compressor: None,
            fill_value: None,
            order: "C".to_string(),
            filters: vec![],
            attributes: HashMap::new(),
            dimensions: vec![],
        }
    }

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
    fn test_is_known_cf_calendar() {
        assert!(is_known_cf_calendar("standard"));
        assert!(is_known_cf_calendar("gregorian"));
        assert!(is_known_cf_calendar("proleptic_gregorian"));
        assert!(is_known_cf_calendar("noleap"));
        assert!(is_known_cf_calendar("365_day"));
        assert!(is_known_cf_calendar("360_day"));

        assert!(!is_known_cf_calendar("custom_calendar"));
    }

    #[test]
    fn test_parse_colon_pairs() {
        let (pairs, warnings) = parse_colon_pairs("area: areacella volume: volcello");
        assert!(warnings.is_empty());
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], ("area".to_string(), "areacella".to_string()));
        assert_eq!(pairs[1], ("volume".to_string(), "volcello".to_string()));

        let (pairs, warnings) = parse_colon_pairs("a: a b: b ps: ps p0: p0");
        assert!(warnings.is_empty());
        assert_eq!(pairs.len(), 4);

        let (pairs, warnings) = parse_colon_pairs("area:");
        assert!(pairs.is_empty());
        assert!(!warnings.is_empty());
    }

    #[test]
    fn test_extract_cell_methods_axes() {
        let axes =
            extract_cell_methods_axes("time: mean (interval: 1 month) area: mean comment: hi");
        assert!(axes.contains("time"));
        assert!(axes.contains("interval"));
        assert!(axes.contains("area"));
        assert!(axes.contains("comment"));
    }

    #[test]
    fn test_is_parametric_vertical_standard_name() {
        assert!(is_parametric_vertical_standard_name(
            "atmosphere_hybrid_sigma_pressure_coordinate"
        ));
        assert!(is_parametric_vertical_standard_name("ocean_s_coordinate"));
        assert!(!is_parametric_vertical_standard_name("air_pressure"));
    }

    #[test]
    fn test_cf_summary_curvilinear_lat_lon_prefers_yx_plot_dims_and_no_dim_coord_warnings() {
        let mut md = ZarrMetadata::new();

        // time(time)
        let mut time = make_var("time", vec![3]);
        time.attributes.insert(
            "_ARRAY_DIMENSIONS".to_string(),
            AttributeValue::Array(vec![AttributeValue::String("time".to_string())]),
        );
        time.attributes.insert(
            "standard_name".to_string(),
            AttributeValue::String("time".to_string()),
        );
        time.attributes.insert(
            "units".to_string(),
            AttributeValue::String("days since 2000-01-01".to_string()),
        );
        md.variables.insert("time".to_string(), time);

        // nav_lat(y,x), nav_lon(y,x)
        let mut nav_lat = make_var("nav_lat", vec![4, 5]);
        nav_lat.attributes.insert(
            "_ARRAY_DIMENSIONS".to_string(),
            AttributeValue::Array(vec![
                AttributeValue::String("y".to_string()),
                AttributeValue::String("x".to_string()),
            ]),
        );
        nav_lat.attributes.insert(
            "standard_name".to_string(),
            AttributeValue::String("latitude".to_string()),
        );
        nav_lat.attributes.insert(
            "units".to_string(),
            AttributeValue::String("degrees_north".to_string()),
        );
        md.variables.insert("nav_lat".to_string(), nav_lat);

        let mut nav_lon = make_var("nav_lon", vec![4, 5]);
        nav_lon.attributes.insert(
            "_ARRAY_DIMENSIONS".to_string(),
            AttributeValue::Array(vec![
                AttributeValue::String("y".to_string()),
                AttributeValue::String("x".to_string()),
            ]),
        );
        nav_lon.attributes.insert(
            "standard_name".to_string(),
            AttributeValue::String("longitude".to_string()),
        );
        nav_lon.attributes.insert(
            "units".to_string(),
            AttributeValue::String("degrees_east".to_string()),
        );
        md.variables.insert("nav_lon".to_string(), nav_lon);

        // temperature(time,y,x) with auxiliary coords
        let mut temperature = make_var("temperature", vec![3, 4, 5]);
        temperature.attributes.insert(
            "_ARRAY_DIMENSIONS".to_string(),
            AttributeValue::Array(vec![
                AttributeValue::String("time".to_string()),
                AttributeValue::String("y".to_string()),
                AttributeValue::String("x".to_string()),
            ]),
        );
        temperature.attributes.insert(
            "coordinates".to_string(),
            AttributeValue::String("nav_lat nav_lon".to_string()),
        );
        md.variables.insert("temperature".to_string(), temperature);

        md.infer_dimensions();

        let summary = cf_summary(&md);

        assert_eq!(
            summary.suggested_plot_dims,
            Some(("y".to_string(), "x".to_string()))
        );
        assert_eq!(summary.suggested_slice_dims, vec!["time".to_string()]);
        assert!(
            summary
                .candidate_data_vars
                .contains(&"temperature".to_string())
        );

        let y_axis = summary.axes.iter().find(|a| a.axis == 'Y').unwrap();
        assert_eq!(y_axis.dims, vec!["y".to_string(), "x".to_string()]);
        assert_eq!(y_axis.coord_var, "nav_lat".to_string());

        let x_axis = summary.axes.iter().find(|a| a.axis == 'X').unwrap();
        assert_eq!(x_axis.dims, vec!["y".to_string(), "x".to_string()]);
        assert_eq!(x_axis.coord_var, "nav_lon".to_string());

        let coord_vars = find_coordinate_variables(&md);
        let mut report = CfReport::default();
        check_dimensions_have_coordinates(&md, &coord_vars, &mut report);

        assert_eq!(
            report.warnings,
            0,
            "unexpected warnings: {:?}",
            report
                .issues
                .iter()
                .map(|i| i.message.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_cf_summary_rectilinear_lat_lon_suggests_lat_lon_plot_dims() {
        let mut md = ZarrMetadata::new();

        let mut time = make_var("time", vec![3]);
        time.attributes.insert(
            "_ARRAY_DIMENSIONS".to_string(),
            AttributeValue::Array(vec![AttributeValue::String("time".to_string())]),
        );
        time.attributes.insert(
            "standard_name".to_string(),
            AttributeValue::String("time".to_string()),
        );
        time.attributes.insert(
            "units".to_string(),
            AttributeValue::String("days since 2000-01-01".to_string()),
        );
        md.variables.insert("time".to_string(), time);

        let mut lat = make_var("lat", vec![4]);
        lat.attributes.insert(
            "_ARRAY_DIMENSIONS".to_string(),
            AttributeValue::Array(vec![AttributeValue::String("lat".to_string())]),
        );
        lat.attributes.insert(
            "standard_name".to_string(),
            AttributeValue::String("latitude".to_string()),
        );
        lat.attributes.insert(
            "units".to_string(),
            AttributeValue::String("degrees_north".to_string()),
        );
        md.variables.insert("lat".to_string(), lat);

        let mut lon = make_var("lon", vec![5]);
        lon.attributes.insert(
            "_ARRAY_DIMENSIONS".to_string(),
            AttributeValue::Array(vec![AttributeValue::String("lon".to_string())]),
        );
        lon.attributes.insert(
            "standard_name".to_string(),
            AttributeValue::String("longitude".to_string()),
        );
        lon.attributes.insert(
            "units".to_string(),
            AttributeValue::String("degrees_east".to_string()),
        );
        md.variables.insert("lon".to_string(), lon);

        let mut temperature = make_var("temperature", vec![3, 4, 5]);
        temperature.attributes.insert(
            "_ARRAY_DIMENSIONS".to_string(),
            AttributeValue::Array(vec![
                AttributeValue::String("time".to_string()),
                AttributeValue::String("lat".to_string()),
                AttributeValue::String("lon".to_string()),
            ]),
        );
        md.variables.insert("temperature".to_string(), temperature);

        md.infer_dimensions();

        let summary = cf_summary(&md);

        assert_eq!(
            summary.suggested_plot_dims,
            Some(("lat".to_string(), "lon".to_string()))
        );
        assert_eq!(summary.suggested_slice_dims, vec!["time".to_string()]);
        assert!(
            summary
                .candidate_data_vars
                .contains(&"temperature".to_string())
        );
    }

    #[test]
    fn test_resolve_related_var_root_and_group() {
        let mut md = ZarrMetadata::new();

        let v_root = make_var("time", vec![10]);
        md.variables.insert("time".to_string(), v_root);

        let mut v_group = make_var("lat", vec![10]);
        v_group.path = "grp/lat".to_string();
        md.variables.insert("grp/lat".to_string(), v_group);

        assert!(resolve_related_var(&md, "temp", "time").is_some());
        assert!(resolve_related_var(&md, "grp/temp", "lat").is_some());
        assert!(resolve_related_var(&md, "grp/temp", "missing").is_none());
    }
}
