# Zarr Summary Tool

A command-line tool for inspecting and summarizing Zarr store metadata, displaying information in a familiar NetCDF-style format.

## Features

- **NetCDF-Style Output**: Displays Zarr metadata in the familiar `ncdump -h` format
- **Consolidated Metadata Support**: Efficiently reads `.zmetadata` files when available
- **Hierarchical Fallback**: Gracefully falls back to scanning individual `.zarray`, `.zattrs`, and `.zgroup` files
- **Dimension Inference**: Automatically detects dimensions and identifies unlimited dimensions
- **Complete Metadata**: Extracts all store information including attributes, data types, and compression settings
- **Colored Output**: Optional syntax highlighting for improved readability

## Installation

```bash
cargo install --path .
```

Or build from source:

```bash
git clone <repository-url>
cd zarr_summery
cargo build --release
```

## Usage

### Basic Usage

```bash
# Inspect a Zarr store
zarr_summery /path/to/zarr/store

# Disable colored output
zarr_summery /path/to/zarr/store --no-color
```

### Example: Climate Data with Hierarchical Metadata

For a typical climate dataset with temperature and pressure arrays:

```bash
zarr_summery ./climate_data/
```

Output:
```
Opening Zarr store: ./climate_data/
Consolidated metadata not found: No consolidated metadata found at './climate_data/.zmetadata'.
Falling back to hierarchical scanning...
netcdf store {
dimensions:
    lat = 180 ;
    level = 50 ;
    lon = 360 ;
    record = UNLIMITED ; // (0 currently)
    time = 365 ;
    x = 100 ;
variables:
    float pressure(time, level, lat, lon) ;
        pressure:_ARRAY_DIMENSIONS = ["time", "level", "lat", "lon"] ;
        pressure:long_name = "Atmospheric Pressure" ;
        pressure:standard_name = "air_pressure" ;
        pressure:units = "hPa" ;
    float temperature(time, lat, lon) ;
        temperature:_ARRAY_DIMENSIONS = ["time", "lat", "lon"] ;
        temperature:long_name = "Air Temperature" ;
        temperature:standard_name = "air_temperature" ;
        temperature:units = "degrees_C" ;
    double unlimited_var(record, x) ;
        unlimited_var:_ARRAY_DIMENSIONS = ["record", "x"] ;
        unlimited_var:long_name = "Unlimited Variable" ;
        unlimited_var:units = "dimensionless" ;
// global attributes:
    :conventions = "CF-1.7" ;
    :history = "Created for demonstration" ;
    :institution = "Research Institute" ;
    :source = "Climate Model v2.1" ;
    :title = "Sample Climate Dataset" ;
}
```

### Example: Consolidated Metadata Store

For stores with consolidated metadata (faster loading):

```bash
zarr_summery ./consolidated_data/
```

Output:
```
Opening Zarr store: ./consolidated_data/
Loaded consolidated metadata from .zmetadata
netcdf store {
dimensions:
    lat = 180 ;
    lon = 360 ;
    time = 365 ;
variables:
    float temperature(time, lat, lon) ;
        temperature:_ARRAY_DIMENSIONS = ["time", "lat", "lon"] ;
        temperature:long_name = "Daily Temperature" ;
        temperature:units = "degrees_C" ;
// global attributes:
    :created = "2024-01-15" ;
    :institution = "Climate Research Center" ;
    :title = "Consolidated Climate Dataset" ;
}
```

### Understanding the Output

- **Dimensions**: Lists all dimensions found across arrays, with unlimited dimensions marked as `UNLIMITED`
- **Variables**: Shows each array with its data type, dimensions, and attributes
- **Global Attributes**: Displays store-level metadata

### Unlimited Dimensions

The tool automatically detects unlimited dimensions by:
1. Finding dimensions that appear with different sizes across multiple arrays
2. Identifying dimensions with size 0 (common for record/time dimensions)

## Supported Zarr Features

### ✅ Supported
- Zarr v2 specification
- Consolidated metadata (`.zmetadata`)
- Hierarchical metadata (`.zarray`, `.zattrs`, `.zgroup`)
- All standard data types (`float32`, `int64`, `bool`, etc.)
- Compression settings (zlib, blosc, zstd, etc.)
- Array attributes and global attributes
- Dimension inference via `_ARRAY_DIMENSIONS`
- Nested groups

### ❌ Current Limitations
- **Zarr v3**: Only Zarr v2 is currently supported
- **Remote Stores**: Only local filesystem stores (no S3, HTTP, etc.)
- **Large Arrays**: No data inspection, metadata only
- **Complex Dtypes**: Basic support for structured dtypes

### 📋 Recommendations
- **Use Consolidated Metadata**: For best performance, create consolidated metadata with `zarr.convenience.consolidate_metadata()`
- **CF Conventions**: Include `_ARRAY_DIMENSIONS` attributes for proper dimension naming

## Architecture

### Core Components

- **`src/main.rs`**: CLI interface and NetCDF-style output formatting
- **`src/metadata.rs`**: Internal metadata structures and parsing
- **`src/store.rs`**: Zarr store interface with consolidated/hierarchical reading
- **`tests/cli.rs`**: Integration tests with sample stores

### Metadata Loading Strategy

1. **Primary**: Attempt to read consolidated metadata from `.zmetadata`
2. **Fallback**: Hierarchical scan of `.zarray`, `.zattrs`, and `.zgroup` files
3. **Dimension Inference**: Build dimension map from `_ARRAY_DIMENSIONS` attributes
4. **Output**: Format in familiar NetCDF `ncdump` style

## Development

### Running Tests

```bash
# Run all tests
cargo test

# Run with output
cargo test -- --nocapture

# Test specific functionality
cargo test test_cli_with_hierarchical_store
```

### Creating Test Stores

See `tests/cli.rs` for examples of creating test Zarr stores programmatically.

## Contributing

Contributions welcome! Areas for improvement:
- Zarr v3 support
- Remote store support (S3, HTTP)
- Performance optimizations
- Additional output formats

## License

MIT License - see LICENSE file for details.
