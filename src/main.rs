mod cf;
mod metadata;
mod plot;
mod store;
mod visualize;

use anyhow::Context;
use clap::{Parser, Subcommand};
use metadata::{AttributeValue, ZarrMetadata};
use std::path::PathBuf;
use std::process;
use store::ZarrStore;

#[derive(Subcommand)]
enum Command {
    /// Check CF conventions (metadata + light-touch coordinate checks)
    CfCheck {
        /// Path to the Zarr store root directory
        path: PathBuf,
    },
}

#[derive(Parser)]
#[command(name = "zarr-dump")]
#[command(version)]
#[command(about = "A tool for summarizing Zarr stores")]
#[command(arg_required_else_help = true)]
#[command(subcommand_precedence_over_arg = true)]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,

    /// Path to the Zarr store root directory (for the default dump/plot mode)
    path: Option<PathBuf>,

    /// Disable colored output
    #[arg(long)]
    no_color: bool,

    /// Show coordinate variable data values (like ncdump -c)
    #[arg(short = 'c', long = "coordinate-data")]
    coordinate_data: bool,

    /// Plot a 2D slice of a variable in an interactive window
    #[arg(long, value_name = "VAR")]
    plot: Option<String>,

    /// Dimensions to plot, formatted as 'dim_y,dim_x'
    #[arg(long, value_name = "DIM_Y,DIM_X", requires = "plot")]
    plot_dims: Option<String>,

    /// Fixed indices for remaining dimensions, formatted as 'dim=index'
    #[arg(long, value_name = "DIM=INDEX", requires = "plot")]
    slice: Vec<String>,
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error: {}", e);

        // Print the error chain for better context
        for cause in e.chain().skip(1) {
            eprintln!("  Caused by: {}", cause);
        }

        process::exit(1);
    }
}

async fn run() -> anyhow::Result<()> {
    let args = Args::parse();

    let (path, mode) = match &args.command {
        Some(Command::CfCheck { path }) => (path.clone(), "cf-check"),
        None => (
            args.path
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Missing Zarr store path"))?,
            "default",
        ),
    };

    // Validate that the path exists and is a directory
    if !path.exists() {
        return Err(anyhow::anyhow!(
            "Zarr store path '{}' does not exist. Please provide a valid path to a Zarr store directory.",
            path.display()
        ));
    }

    if !path.is_dir() {
        return Err(anyhow::anyhow!(
            "Path '{}' is not a directory. Zarr stores must be directories containing .zarray, .zgroup, or .zmetadata files.",
            path.display()
        ));
    }

    println!("Opening Zarr store: {}", path.display());

    // Create and load Zarr store
    let store = ZarrStore::new(&path)?;
    let metadata = store
        .load_metadata()
        .await
        .with_context(|| format!("Failed to load Zarr store from '{}'", path.display()))?;

    if mode == "cf-check" {
        let report = cf::cf_check(&store, &metadata).await?;
        report.print();
        if report.has_errors() {
            return Err(anyhow::anyhow!("CF check failed"));
        }
        return Ok(());
    }

    if let Some(plot_var) = &args.plot {
        if args.command.is_some() {
            return Err(anyhow::anyhow!(
                "Plotting is only supported in the default mode. Use `zarr-dump STORE --plot ...`, not a subcommand."
            ));
        }
        if args.coordinate_data {
            return Err(anyhow::anyhow!(
                "--coordinate-data (-c) is not supported with --plot. Run the commands separately."
            ));
        }

        let plot_dims = args
            .plot_dims
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("--plot-dims is required when using --plot"))?;
        let (dim_y, dim_x) = plot::parse_plot_dims(plot_dims)?;
        let slices = plot::parse_slices(&args.slice)?;

        let var_key = normalize_plot_variable_key(plot_var);
        let variable = metadata.variables.get(&var_key).ok_or_else(|| {
            let mut keys: Vec<&String> = metadata.variables.keys().collect();
            keys.sort();
            let shown = keys.len().min(20);
            let preview = keys
                .into_iter()
                .take(shown)
                .map(|k| if k.is_empty() { "root" } else { k.as_str() })
                .collect::<Vec<_>>()
                .join(", ");

            anyhow::anyhow!(
                "Variable '{}' not found in store. Available variables (first {}): {}",
                plot_var,
                shown,
                preview
            )
        })?;

        let selection = plot::build_plot_selection(variable, &dim_y, &dim_x, &slices)?;
        let data = store
            .read_array_subset_f64(variable, &selection.ranges)
            .with_context(|| format!("Failed to read data for variable '{}'", plot_var))?;

        let title_base = format!(
            "{}: {},{}",
            if var_key.is_empty() { "root" } else { plot_var },
            selection.dim_y_name,
            selection.dim_x_name
        );

        let mut nav_dims = Vec::new();
        for (i, dim) in variable.dimensions.iter().enumerate() {
            if dim.name == dim_y || dim.name == dim_x {
                continue;
            }

            let idx = *slices
                .get(&dim.name)
                .expect("slice indices should be validated by build_plot_selection");
            let size = variable.shape[i];
            if size == 0 {
                return Err(anyhow::anyhow!(
                    "Dimension '{}' has length 0 in variable '{}' (cannot navigate).",
                    dim.name,
                    variable.name
                ));
            }

            nav_dims.push(visualize::SliceDimension {
                name: dim.name.clone(),
                index: idx,
                max: size - 1,
            });
        }

        let view = visualize::ImageView {
            width: selection.width,
            height: selection.height,
            stride_y: selection.stride_y,
            stride_x: selection.stride_x,
        };

        if nav_dims.is_empty() {
            visualize::show_viridis_image(&title_base, &data, view)?;
        } else {
            println!(
                "Navigation: Tab=next dim, ←/→ or ↑/↓=±1 (Shift=×10), PgUp/PgDn=±10 (Shift=×100), Home/End=min/max, Esc/q=quit"
            );
            visualize::show_viridis_image_with_navigation(
                &title_base,
                data,
                view,
                nav_dims,
                |dims| {
                    let slices: std::collections::HashMap<String, u64> =
                        dims.iter().map(|d| (d.name.clone(), d.index)).collect();
                    let selection = plot::build_plot_selection(variable, &dim_y, &dim_x, &slices)?;
                    store.read_array_subset_f64(variable, &selection.ranges)
                },
            )?;
        }
    } else {
        if args.command.is_some() {
            return Err(anyhow::anyhow!(
                "This subcommand does not support metadata dump output."
            ));
        }
        print_metadata_summary(&metadata, args.no_color, args.coordinate_data, &store).await?;
    }

    Ok(())
}

fn normalize_plot_variable_key(raw: &str) -> String {
    let s = raw.trim();
    if s == "/" || s.eq_ignore_ascii_case("root") {
        return String::new();
    }
    s.trim_start_matches('/').to_string()
}

async fn print_metadata_summary(
    metadata: &ZarrMetadata,
    no_color: bool,
    coordinate_data: bool,
    store: &ZarrStore,
) -> anyhow::Result<()> {
    let formatter = NetCdfFormatter::new(!no_color);
    formatter
        .print_header(metadata, coordinate_data, store)
        .await?;
    Ok(())
}

/// NetCDF-style header formatter with color support
struct NetCdfFormatter {
    use_color: bool,
}

impl NetCdfFormatter {
    fn new(use_color: bool) -> Self {
        Self { use_color }
    }

    async fn print_header(
        &self,
        metadata: &ZarrMetadata,
        coordinate_data: bool,
        store: &ZarrStore,
    ) -> anyhow::Result<()> {
        let store_name = "store"; // Could be extracted from path if needed

        // Header
        println!(
            "{} {} {{",
            self.colorize("zarr", "34"),    // Blue for keyword
            self.colorize(store_name, "1")  // Bold for store name
        );

        // Dimensions section
        self.print_dimensions(metadata);

        // Variables section
        self.print_variables(metadata);

        // Global attributes section
        self.print_global_attributes(metadata);

        // CF summary section
        self.print_cf_summary(metadata);

        // Coordinate data section (if requested)
        if coordinate_data {
            self.print_coordinate_data(metadata, store).await?;
        }

        // Closing brace
        println!("}}");
        Ok(())
    }

    fn print_dimensions(&self, metadata: &ZarrMetadata) {
        if metadata.dimensions.is_empty() {
            return;
        }

        println!("{}", self.colorize("dimensions:", "32")); // Green for section headers

        let mut sorted_dims: Vec<_> = metadata.dimensions.iter().collect();
        sorted_dims.sort_by_key(|(name, _)| name.as_str());

        for (name, dim_info) in sorted_dims {
            if dim_info.is_unlimited {
                println!(
                    "    {} = {} ; {}",
                    self.colorize(name, "36"), // Cyan for dimension names
                    self.colorize("UNLIMITED", "33"), // Yellow for UNLIMITED
                    self.colorize(&format!("// ({} currently)", dim_info.max_length), "90") // Gray for comments
                );
            } else {
                println!(
                    "    {} = {} ;",
                    self.colorize(name, "36"), // Cyan for dimension names
                    self.colorize(&format!("{}", dim_info.max_length), "33")  // Yellow for values
                );
            }
        }
    }

    fn print_variables(&self, metadata: &ZarrMetadata) {
        if metadata.variables.is_empty() {
            return;
        }

        println!("{}", self.colorize("variables:", "32")); // Green for section headers

        let mut sorted_vars: Vec<_> = metadata.variables.iter().collect();
        sorted_vars.sort_by_key(|(path, _)| path.as_str());

        for (path, variable) in sorted_vars.iter() {
            // Variable declaration line
            let var_name = if path.is_empty() {
                "root"
            } else {
                &variable.name
            };
            let dims_str = self.format_variable_dimensions(&variable.dimensions);

            println!(
                "    {} {}({}) ;",
                self.colorize(&self.map_dtype_to_netcdf(&variable.dtype), "35"), // Magenta for data types
                self.colorize(var_name, "36"), // Cyan for variable names
                dims_str
            );

            // Variable attributes
            self.print_variable_attributes(var_name, &variable.attributes);
        }
    }

    fn print_variable_attributes(
        &self,
        var_name: &str,
        attributes: &std::collections::HashMap<String, AttributeValue>,
    ) {
        let mut sorted_attrs: Vec<_> = attributes.iter().collect();
        sorted_attrs.sort_by_key(|(key, _)| key.as_str());

        for (key, value) in sorted_attrs {
            let formatted_value = self.format_attribute_value(value);
            println!(
                "        {}:{} = {} ;",
                self.colorize(var_name, "36"), // Cyan for variable name
                self.colorize(key, "33"),      // Yellow for attribute name
                formatted_value
            );
        }
    }

    fn print_global_attributes(&self, metadata: &ZarrMetadata) {
        if metadata.global_attributes.is_empty() {
            return;
        }

        println!("{}", self.colorize("// global attributes:", "90")); // Gray for comments

        let mut sorted_attrs: Vec<_> = metadata.global_attributes.iter().collect();
        sorted_attrs.sort_by_key(|(key, _)| key.as_str());

        for (key, value) in sorted_attrs {
            let formatted_value = self.format_attribute_value(value);
            println!(
                "    :{} = {} ;",
                self.colorize(key, "33"), // Yellow for attribute name
                formatted_value
            );
        }
    }

    fn print_cf_summary(&self, metadata: &ZarrMetadata) {
        let summary = cf::cf_summary(metadata);

        println!("{}", self.colorize("// CF summary:", "90"));

        match &summary.conventions {
            Some(s) => println!("    // Conventions: {}", self.colorize(s, "31")),
            None => println!("    // Conventions: <missing>"),
        }

        if summary.axes.is_empty() {
            println!("    // Axes: <none detected>");
        } else {
            println!("    // Axes:");
            for axis in &summary.axes {
                println!(
                    "    //   {}: {} (dim '{}')",
                    self.colorize(&axis.axis.to_string(), "33"),
                    self.colorize(&axis.coord_var, "36"),
                    self.colorize(&axis.dim, "36")
                );
            }
        }

        if let Some((dim_y, dim_x)) = &summary.suggested_plot_dims {
            println!(
                "    // Suggested plot dims: {},{}",
                self.colorize(dim_y, "36"),
                self.colorize(dim_x, "36")
            );
        }

        if !summary.suggested_slice_dims.is_empty() {
            println!(
                "    // Suggested slice dims: {}",
                summary
                    .suggested_slice_dims
                    .iter()
                    .map(|d| self.colorize(d, "36"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        if !summary.candidate_data_vars.is_empty() {
            println!(
                "    // Candidate data variables: {}",
                summary
                    .candidate_data_vars
                    .iter()
                    .map(|v| self.colorize(v, "36"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }

    fn format_variable_dimensions(&self, dimensions: &[metadata::Dimension]) -> String {
        let dim_names: Vec<String> = dimensions
            .iter()
            .map(|d| {
                self.colorize(&d.name, "36") // Cyan for dimension names
            })
            .collect();
        dim_names.join(", ")
    }

    fn format_attribute_value(&self, value: &AttributeValue) -> String {
        match value {
            AttributeValue::String(s) => {
                // Handle string quoting and escaping
                let escaped = s.replace('\"', "\\\"").replace('\n', "\\n");
                self.colorize(&format!("\"{}\"", escaped), "31") // Red for strings
            }
            AttributeValue::Number(n) => {
                self.colorize(&format!("{}", n), "33") // Yellow for numbers
            }
            AttributeValue::Integer(i) => {
                self.colorize(&format!("{}", i), "33") // Yellow for numbers
            }
            AttributeValue::Boolean(b) => {
                self.colorize(&format!("{}", b), "35") // Magenta for booleans
            }
            AttributeValue::Array(arr) => {
                let elements: Vec<String> = arr
                    .iter()
                    .take(5) // Limit array display
                    .map(|v| self.format_attribute_value(v))
                    .collect();
                let array_str = if arr.len() > 5 {
                    format!("[{}, ...]", elements.join(", "))
                } else {
                    format!("[{}]", elements.join(", "))
                };
                self.colorize(&array_str, "37") // White for arrays
            }
            AttributeValue::Object(_) => {
                self.colorize("{...}", "37") // White for objects
            }
            AttributeValue::Null => {
                self.colorize("null", "90") // Gray for null
            }
        }
    }

    fn map_dtype_to_netcdf(&self, dtype: &str) -> String {
        // Map Zarr dtypes to NetCDF-like types
        match dtype {
            s if s.starts_with("|S") || s.starts_with("|U") || s == "<U1" => "char".to_string(),
            "<i1" | ">i1" | "|i1" => "byte".to_string(),
            "<i2" | ">i2" => "short".to_string(),
            "<i4" | ">i4" => "int".to_string(),
            "<i8" | ">i8" => "int64".to_string(),
            "<u1" | ">u1" | "|u1" => "ubyte".to_string(),
            "<u2" | ">u2" => "ushort".to_string(),
            "<u4" | ">u4" => "uint".to_string(),
            "<u8" | ">u8" => "uint64".to_string(),
            "<f4" | ">f4" => "float".to_string(),
            "<f8" | ">f8" => "double".to_string(),
            "<c8" | ">c8" => "complex64".to_string(),
            "<c16" | ">c16" => "complex128".to_string(),
            "?" => "bool".to_string(),
            other => other.to_string(), // Fallback to original
        }
    }

    async fn print_coordinate_data(
        &self,
        metadata: &ZarrMetadata,
        store: &ZarrStore,
    ) -> anyhow::Result<()> {
        // Identify coordinate variables (1D variables that match dimension names)
        let coordinate_vars: Vec<(&String, &metadata::Variable)> = metadata
            .variables
            .iter()
            .filter(|(_, var)| {
                // A coordinate variable is typically 1D and its name matches a dimension
                var.dimensions.len() == 1
                    && metadata.dimensions.contains_key(&var.dimensions[0].name)
            })
            .collect();

        if coordinate_vars.is_empty() {
            return Ok(());
        }

        println!("{}", self.colorize("data:", "32")); // Green for section header
        println!();

        // Read and display coordinate data
        for (_, var) in coordinate_vars {
            match store.read_coordinate_data(var).await {
                Ok(data) => {
                    let formatted_data = self.format_coordinate_values(&data);
                    println!(" {} = {} ;", self.colorize(&var.name, "36"), formatted_data);
                    println!(); // Add blank line between variables
                }
                Err(e) => {
                    // If we can't read the data, show an error message but continue
                    println!(
                        " {} = {} ;",
                        self.colorize(&var.name, "36"),
                        self.colorize(&format!("<error reading data: {}>", e), "31")
                    );
                }
            }
        }

        Ok(())
    }

    fn format_coordinate_values(&self, data: &[f64]) -> String {
        const MAX_VALUES_PER_LINE: usize = 8;
        const LINE_WIDTH: usize = 76;

        if data.is_empty() {
            return self.colorize("<no data>", "90");
        }

        let mut lines = Vec::new();
        let mut current_line = String::new();
        let mut values_on_line = 0;

        for (i, &value) in data.iter().enumerate() {
            let formatted_val = if value.fract() == 0.0 && value.abs() < 1e10 {
                format!("{}", value as i64)
            } else if value.abs() >= 1e6 || (value.abs() < 1e-3 && value != 0.0) {
                format!("{:e}", value)
            } else {
                format!("{}", value)
            };

            let val_str = if i == data.len() - 1 {
                // Last value, no comma
                formatted_val
            } else {
                format!("{}, ", formatted_val)
            };

            // Check if adding this value would exceed line width or max values per line
            if (values_on_line >= MAX_VALUES_PER_LINE
                || (current_line.len() + val_str.len()) > LINE_WIDTH)
                && !current_line.is_empty()
            {
                lines.push(current_line.trim_end_matches(", ").to_string());
                current_line = String::new();
                values_on_line = 0;
            }

            current_line.push_str(&val_str);
            values_on_line += 1;
        }

        // Add the remaining line
        if !current_line.is_empty() {
            lines.push(current_line.trim_end_matches(", ").to_string());
        }

        // Join lines with proper indentation (like ncdump)
        if lines.len() == 1 {
            self.colorize(&lines[0], "33") // Yellow for values
        } else {
            let first_line = self.colorize(&lines[0], "33");
            let remaining_lines: Vec<String> = lines[1..]
                .iter()
                .map(|line| format!("    {}", self.colorize(line, "33")))
                .collect();

            if remaining_lines.is_empty() {
                first_line
            } else {
                format!("{}, \n{}", first_line, remaining_lines.join(", \n"))
            }
        }
    }

    fn colorize(&self, text: &str, color_code: &str) -> String {
        if self.use_color {
            format!("\x1b[{}m{}\x1b[0m", color_code, text)
        } else {
            text.to_string()
        }
    }
}
