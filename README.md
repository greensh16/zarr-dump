# zarr-dump

A CLI tool for inspecting Zarr v2/v3 stores and printing their metadata in a familiar NetCDF-style header format (similar to `ncdump -h`).

It also includes CF-focused tooling aimed at climate/ocean/atmosphere/land datasets: a `cf-check` subcommand and a `// CF summary:` section in the default output.

## Features

- **NetCDF-style metadata output** (dimensions, variables, attributes)
- **Zarr v2 and v3 support**
  - Reads consolidated metadata when present (`.zmetadata` for v2, or `consolidated_metadata` in v3 `zarr.json`)
  - Falls back to hierarchical scanning when consolidated metadata isn’t available
- **Dimension inference** (from `_ARRAY_DIMENSIONS` / `dimension_names`) and **unlimited dimension detection**
- **CF conventions tooling**
  - **CF summary** in the default dump output:
    - Detects axes (T/Z/Y/X) and works with both rectilinear and curvilinear grids (e.g. 2D `lat(y,x)` / `lon(y,x)` referenced via `coordinates`)
    - Suggests plot dims and slice dims
    - Lists candidate data variables (excluding coordinate-like variables)
  - **`cf-check` subcommand** with metadata + light-touch coordinate checks (exits non-zero if any errors are found)
- **Coordinate data display** (`-c/--coordinate-data`, like `ncdump -c`) for 1D coordinate variables (best-effort)
- **2D slice plotting** in an interactive window (viridis colormap)
  - `zarr-dump STORE --plot VAR --plot-dims dim_y,dim_x --slice dim=index ...`
  - Optional in-window navigation for non-plotted dimensions
- **Optional colored output** (`--no-color` to disable)

## Installation

Build from source:

```bash
git clone <repository-url>
cd zarr-dump
cargo build --release
```

Install locally (optional):

```bash
cargo install --path .
```

## Usage

### Dump metadata (default mode)

```bash
# Inspect a Zarr store
zarr-dump /path/to/zarr/store

# Disable colored output
zarr-dump /path/to/zarr/store --no-color

# Include coordinate variable values (like ncdump -c)
zarr-dump /path/to/zarr/store -c
```

The default output includes:
- `dimensions:`
- `variables:`
- `// global attributes:`
- `// CF summary:`
- optional `data:` (when `-c/--coordinate-data` is set)

### CF checks

```bash
# Run CF-focused checks (exits non-zero if any ERRORs are present)
zarr-dump cf-check /path/to/zarr/store
```

Checks include (high level):
- `Conventions`/`conventions` presence and CF-like content
- Dimension name list validation (`_ARRAY_DIMENSIONS` / `dimension_names`)
- Coordinate-variable checks (units type, bounds references, sampled monotonicity where readable)
- Time/vertical/lat/lon semantics checks (e.g. CF-style time units, calendar values, `positive`, lat/lon ranges)
- Reference attribute checks (`coordinates`, `ancillary_variables`, `cell_measures`, `cell_methods`, `grid_mapping`, `formula_terms`)
- Auxiliary 2D lat/lon detection and validation (including whether they’re referenced via `coordinates`)

### Plot a 2D slice

```bash
# Plot a 2D slice of a variable (requires exactly 2 plotted dims)
# Provide --slice for every remaining dimension not included in --plot-dims.
zarr-dump /path/to/zarr/store \
  --plot temperature \
  --plot-dims y,x \
  --slice time=0
```

If the variable has extra dimensions (beyond the two plotted dims), the viewer supports interactive navigation:

- Tab: next dimension
- ←/→ or ↑/↓: ±1 (Shift = ×10)
- PgUp/PgDn: ±10 (Shift = ×100)
- Home/End: min/max
- Esc/q: quit

Notes:
- Plotting is only supported in the default mode (`zarr-dump STORE --plot ...`).
- `-c/--coordinate-data` is not supported together with `--plot`.

## Example output (abridged)

```text
Opening Zarr store: ./climate_data/
zarr store {
dimensions:
    lat = 180 ;
    lon = 360 ;
    time = 365 ;
variables:
    float temperature(time, lat, lon) ;
        temperature:standard_name = "air_temperature" ;
        temperature:units = "K" ;
// global attributes:
    :Conventions = "CF-1.8" ;
// CF summary:
    // Conventions: CF-1.8
    // Axes:
    //   T: time (dims 'time')
    //   Y: lat (dims 'lat')
    //   X: lon (dims 'lon')
    // Suggested plot dims: lat,lon
    // Suggested slice dims: time
    // Candidate data variables: temperature
}
```

## Supported Zarr Features

- Zarr v2 and v3 specifications
- **v2**: consolidated metadata (`.zmetadata`) and hierarchical metadata (`.zarray`, `.zattrs`, `.zgroup`)
- **v3**: `zarr.json` at the root and per-array `zarr.json`
- Array and global attributes
- Nested groups
- Common primitive dtypes (e.g. int/float/bool/strings) and dtype display
- Compression/filter metadata reporting

## Limitations

- Local filesystem stores only (no S3/HTTP/GCS)
- Data inspection is intentionally limited:
  - `-c/--coordinate-data` reads and prints 1D coordinate variable values
  - `--plot` reads and displays a numeric 2D slice
- Complex/structured dtypes have limited support (especially for data reading)

## Architecture

Core modules:

- `src/main.rs`: CLI and formatting (`NetCdfFormatter`)
- `src/store.rs`: metadata loading (consolidated + hierarchical fallback) and data reads used by `-c`/plotting
- `src/metadata.rs`: internal metadata structures and dimension inference
- `src/cf.rs`: CF summary + `cf-check` implementation
- `src/plot.rs` and `src/visualize.rs`: 2D slice plotting + navigation

## Development

```bash
# Run tests
cargo test

# Format
cargo fmt

# Lint
cargo clippy -- -D warnings
```

## License

MIT License - see LICENSE file for details.
