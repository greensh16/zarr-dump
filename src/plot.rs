use crate::metadata::Variable;
use anyhow::{Context, Result, anyhow, bail};
use std::collections::{HashMap, HashSet};
use std::ops::Range;

#[derive(Debug, Clone)]
pub struct PlotSelection {
    pub dim_y_name: String,
    pub dim_x_name: String,
    pub height: usize,
    pub width: usize,
    pub stride_y: usize,
    pub stride_x: usize,
    pub ranges: Vec<Range<u64>>,
}

pub fn parse_plot_dims(raw: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = raw
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    if parts.len() != 2 {
        bail!(
            "Invalid --plot-dims '{}'. Expected 'dim_y,dim_x' (two comma-separated dimension names).",
            raw
        );
    }

    Ok((parts[0].to_string(), parts[1].to_string()))
}

pub fn parse_slices(values: &[String]) -> Result<HashMap<String, u64>> {
    let mut slices: HashMap<String, u64> = HashMap::new();

    for raw in values {
        let (name, index_str) = raw
            .split_once('=')
            .ok_or_else(|| anyhow!("Invalid --slice '{}'. Expected 'dim=index'.", raw))?;

        let name = name.trim();
        let index_str = index_str.trim();
        if name.is_empty() || index_str.is_empty() {
            bail!("Invalid --slice '{}'. Expected 'dim=index'.", raw);
        }

        let index: u64 = index_str
            .parse()
            .with_context(|| format!("Invalid index in --slice '{}'. Expected an integer.", raw))?;

        if slices.insert(name.to_string(), index).is_some() {
            bail!("Duplicate --slice provided for dimension '{}'.", name);
        }
    }

    Ok(slices)
}

pub fn build_plot_selection(
    variable: &Variable,
    dim_y: &str,
    dim_x: &str,
    slices: &HashMap<String, u64>,
) -> Result<PlotSelection> {
    if variable.order != "C" {
        bail!(
            "Plotting currently only supports C-order arrays (order='C'). Variable '{}' has order='{}'.",
            variable.name,
            variable.order
        );
    }

    let ndims = variable.shape.len();
    if ndims < 2 {
        bail!(
            "Cannot plot variable '{}' because it has {} dimensions (need at least 2).",
            variable.name,
            ndims
        );
    }

    let dim_names = dimension_names(variable);
    let dim_name_set: HashSet<&str> = dim_names.iter().map(|s| s.as_str()).collect();

    // Validate slice keys early to produce clear errors.
    for key in slices.keys() {
        if !dim_name_set.contains(key.as_str()) {
            bail!(
                "Unknown dimension '{}' in --slice for variable '{}'. Available dimensions: {}",
                key,
                variable.name,
                dim_names.join(", ")
            );
        }
    }

    if slices.contains_key(dim_y) || slices.contains_key(dim_x) {
        bail!(
            "Do not provide --slice for plotted dimensions ('{}' and '{}').",
            dim_y,
            dim_x
        );
    }

    let dim_y_idx = dim_names.iter().position(|d| d == dim_y).ok_or_else(|| {
        anyhow!(
            "Unknown y dimension '{}' for variable '{}'. Available dimensions: {}",
            dim_y,
            variable.name,
            dim_names.join(", ")
        )
    })?;

    let dim_x_idx = dim_names.iter().position(|d| d == dim_x).ok_or_else(|| {
        anyhow!(
            "Unknown x dimension '{}' for variable '{}'. Available dimensions: {}",
            dim_x,
            variable.name,
            dim_names.join(", ")
        )
    })?;

    if dim_x_idx == dim_y_idx {
        bail!("--plot-dims must specify two different dimensions.");
    }

    // Collect missing slice dimensions so we can present a single actionable error.
    let mut missing = Vec::new();
    for (i, name) in dim_names.iter().enumerate() {
        if i == dim_y_idx || i == dim_x_idx {
            continue;
        }
        if !slices.contains_key(name) {
            missing.push(name.clone());
        }
    }

    if !missing.is_empty() {
        bail!(
            "Missing --slice for dimensions: {}. Provide an index for every dimension not included in --plot-dims.",
            missing.join(", ")
        );
    }

    // Build ranges and subset shape.
    let mut ranges: Vec<Range<u64>> = Vec::with_capacity(ndims);
    let mut subset_shape: Vec<usize> = Vec::with_capacity(ndims);

    for (i, name) in dim_names.iter().enumerate() {
        let size = variable.shape[i];
        if size == 0 {
            bail!(
                "Dimension '{}' has length 0 in variable '{}' (cannot plot).",
                name,
                variable.name
            );
        }

        if i == dim_y_idx || i == dim_x_idx {
            ranges.push(0..size);
            subset_shape.push(usize::try_from(size).with_context(|| {
                format!(
                    "Dimension '{}' is too large to plot on this platform (size {}).",
                    name, size
                )
            })?);
        } else {
            let idx = slices[name];
            if idx >= size {
                bail!(
                    "Index {} out of bounds for dimension '{}' (valid range: 0..{}).",
                    idx,
                    name,
                    size - 1
                );
            }
            ranges.push(idx..idx + 1);
            subset_shape.push(1);
        }
    }

    let width = subset_shape[dim_x_idx];
    let height = subset_shape[dim_y_idx];

    let strides = compute_c_strides(&subset_shape)?;

    Ok(PlotSelection {
        dim_y_name: dim_y.to_string(),
        dim_x_name: dim_x.to_string(),
        height,
        width,
        stride_y: strides[dim_y_idx],
        stride_x: strides[dim_x_idx],
        ranges,
    })
}

fn dimension_names(variable: &Variable) -> Vec<String> {
    if variable.dimensions.len() == variable.shape.len() && !variable.dimensions.is_empty() {
        variable.dimensions.iter().map(|d| d.name.clone()).collect()
    } else {
        (0..variable.shape.len())
            .map(|i| format!("dim_{}", i))
            .collect()
    }
}

fn compute_c_strides(shape: &[usize]) -> Result<Vec<usize>> {
    let mut strides = vec![1usize; shape.len()];
    let mut stride = 1usize;

    for i in (0..shape.len()).rev() {
        strides[i] = stride;
        stride = stride.checked_mul(shape[i]).ok_or_else(|| {
            anyhow!("Array subset is too large to index (overflow computing strides).")
        })?;
    }

    Ok(strides)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{Dimension, Variable};
    use std::collections::HashMap;

    fn make_var(dim_names: &[&str], shape: &[u64]) -> Variable {
        let dimensions = dim_names
            .iter()
            .zip(shape)
            .map(|(&name, &size)| Dimension {
                name: name.to_string(),
                size,
                is_unlimited: false,
            })
            .collect();

        Variable {
            name: "v".to_string(),
            path: "v".to_string(),
            dtype: "<f4".to_string(),
            shape: shape.to_vec(),
            chunks: vec![],
            compressor: None,
            fill_value: None,
            order: "C".to_string(),
            filters: vec![],
            attributes: HashMap::new(),
            dimensions,
        }
    }

    #[test]
    fn test_parse_plot_dims() {
        let (y, x) = parse_plot_dims("lat,lon").unwrap();
        assert_eq!(y, "lat");
        assert_eq!(x, "lon");

        assert!(parse_plot_dims("lat").is_err());
        assert!(parse_plot_dims("lat,lon,time").is_err());
    }

    #[test]
    fn test_parse_slices() {
        let slices = parse_slices(&["time=0".to_string(), "level=3".to_string()]).unwrap();
        assert_eq!(slices["time"], 0);
        assert_eq!(slices["level"], 3);

        assert!(parse_slices(&["time".to_string()]).is_err());
        assert!(parse_slices(&["time=".to_string()]).is_err());
        assert!(parse_slices(&["=0".to_string()]).is_err());
    }

    #[test]
    fn test_build_plot_selection_lat_lon_time_slice() {
        let var = make_var(&["time", "lat", "lon"], &[365, 180, 360]);
        let mut slices = HashMap::new();
        slices.insert("time".to_string(), 0);

        let sel = build_plot_selection(&var, "lat", "lon", &slices).unwrap();
        assert_eq!(sel.height, 180);
        assert_eq!(sel.width, 360);

        // shape [1, 180, 360] -> strides [64800, 360, 1]
        assert_eq!(sel.stride_y, 360);
        assert_eq!(sel.stride_x, 1);

        assert_eq!(sel.ranges.len(), 3);
        assert_eq!(sel.ranges[0], 0..1);
        assert_eq!(sel.ranges[1], 0..180);
        assert_eq!(sel.ranges[2], 0..360);
    }

    #[test]
    fn test_build_plot_selection_transposed_dims() {
        let var = make_var(&["x", "y"], &[4, 3]);
        let slices = HashMap::new();

        // Plot y as vertical and x as horizontal, even though x comes first.
        let sel = build_plot_selection(&var, "y", "x", &slices).unwrap();
        assert_eq!(sel.height, 3);
        assert_eq!(sel.width, 4);

        // subset shape [4, 3] -> strides [3, 1]
        assert_eq!(sel.stride_y, 1);
        assert_eq!(sel.stride_x, 3);
    }

    #[test]
    fn test_build_plot_selection_missing_slice() {
        let var = make_var(&["time", "lat", "lon"], &[365, 180, 360]);
        let slices = HashMap::new();
        let err = build_plot_selection(&var, "lat", "lon", &slices).unwrap_err();
        assert!(err.to_string().contains("Missing --slice"));
    }
}
