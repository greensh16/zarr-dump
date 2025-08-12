mod metadata;
mod store;

use anyhow::Context;
use clap::Parser;
use metadata::{AttributeValue, ZarrMetadata};
use std::path::PathBuf;
use std::process;
use store::ZarrStore;

#[derive(Parser)]
#[command(name = "zarr-dump")]
#[command(version)]
#[command(about = "A tool for summarizing Zarr stores")]
struct Args {
    /// Path to the Zarr store root directory
    path: PathBuf,

    /// Disable colored output
    #[arg(long)]
    no_color: bool,

    /// Show coordinate variable data values (like ncdump -c)
    #[arg(short = 'c', long = "coordinate-data")]
    coordinate_data: bool,
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

    // Validate that the path exists and is a directory
    if !args.path.exists() {
        return Err(anyhow::anyhow!(
            "Zarr store path '{}' does not exist. Please provide a valid path to a Zarr store directory.",
            args.path.display()
        ));
    }

    if !args.path.is_dir() {
        return Err(anyhow::anyhow!(
            "Path '{}' is not a directory. Zarr stores must be directories containing .zarray, .zgroup, or .zmetadata files.",
            args.path.display()
        ));
    }

    println!("Opening Zarr store: {}", args.path.display());

    // Create and load Zarr store
    let store = ZarrStore::new(&args.path)?;
    let metadata = store
        .load_metadata()
        .await
        .with_context(|| format!("Failed to load Zarr store from '{}'", args.path.display()))?;

    print_metadata_summary(&metadata, args.no_color, args.coordinate_data, &store).await?;

    Ok(())
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
