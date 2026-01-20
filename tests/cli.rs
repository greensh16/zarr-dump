use std::fs;
use std::path::Path;
use std::process::Command;
use tempfile::TempDir;

/// Helper function to create a sample Zarr store for testing
fn create_sample_store(temp_dir: &Path) -> std::io::Result<()> {
    // Create .zgroup for root
    let zgroup_content = r#"{"zarr_format": 2}"#;
    fs::write(temp_dir.join(".zgroup"), zgroup_content)?;

    // Create .zattrs for root (global attributes)
    let zattrs_content = r#"{
        "title": "Sample Climate Dataset",
        "institution": "Test University",
        "source": "zarr-dump integration test",
        "history": "Created for testing"
    }"#;
    fs::write(temp_dir.join(".zattrs"), zattrs_content)?;

    // Create temperature variable
    fs::create_dir_all(temp_dir.join("temperature"))?;

    let temp_zarray = r#"{
        "zarr_format": 2,
        "shape": [365, 180, 360],
        "chunks": [1, 180, 360],
        "dtype": "<f4",
        "compressor": {"id": "zlib", "level": 1},
        "fill_value": -9999.0,
        "order": "C",
        "filters": null
    }"#;
    fs::write(temp_dir.join("temperature").join(".zarray"), temp_zarray)?;

    let temp_zattrs = r#"{
        "long_name": "Air Temperature",
        "units": "degrees_C",
        "standard_name": "air_temperature",
        "_ARRAY_DIMENSIONS": ["time", "lat", "lon"]
    }"#;
    fs::write(temp_dir.join("temperature").join(".zattrs"), temp_zattrs)?;

    // Create pressure variable with different dimensions
    fs::create_dir_all(temp_dir.join("pressure"))?;

    let pressure_zarray = r#"{
        "zarr_format": 2,
        "shape": [365, 50, 180, 360],
        "chunks": [1, 50, 180, 360],
        "dtype": "<f4",
        "compressor": {"id": "blosc", "cname": "zstd", "clevel": 3, "shuffle": 1},
        "fill_value": null,
        "order": "C",
        "filters": null
    }"#;
    fs::write(temp_dir.join("pressure").join(".zarray"), pressure_zarray)?;

    let pressure_zattrs = r#"{
        "long_name": "Atmospheric Pressure",
        "units": "hPa",
        "standard_name": "air_pressure",
        "_ARRAY_DIMENSIONS": ["time", "level", "lat", "lon"]
    }"#;
    fs::write(temp_dir.join("pressure").join(".zattrs"), pressure_zattrs)?;

    // Create a variable with unlimited dimension
    fs::create_dir_all(temp_dir.join("unlimited_var"))?;

    let unlimited_zarray = r#"{
        "zarr_format": 2,
        "shape": [0, 100],
        "chunks": [1, 100],
        "dtype": "<f8",
        "compressor": null,
        "fill_value": null,
        "order": "C",
        "filters": null
    }"#;
    fs::write(
        temp_dir.join("unlimited_var").join(".zarray"),
        unlimited_zarray,
    )?;

    let unlimited_zattrs = r#"{
        "long_name": "Unlimited Variable",
        "units": "dimensionless",
        "_ARRAY_DIMENSIONS": ["record", "x"]
    }"#;
    fs::write(
        temp_dir.join("unlimited_var").join(".zattrs"),
        unlimited_zattrs,
    )?;

    Ok(())
}

/// Helper function to create a consolidated metadata store
fn create_consolidated_store(temp_dir: &Path) -> std::io::Result<()> {
    let consolidated_metadata = r#"{
        "zarr_consolidated_format": 1,
        "metadata": {
            ".zattrs": {
                "title": "Consolidated Sample Dataset",
                "creation_date": "2024-01-01"
            },
            ".zgroup": {
                "zarr_format": 2
            },
            "data/.zarray": {
                "zarr_format": 2,
                "shape": [100, 200],
                "chunks": [10, 20],
                "dtype": "<i4",
                "compressor": {"id": "zstd", "level": 3},
                "fill_value": 0,
                "order": "C",
                "filters": null
            },
            "data/.zattrs": {
                "long_name": "Sample Data Array",
                "units": "counts",
                "_ARRAY_DIMENSIONS": ["y", "x"]
            }
        }
    }"#;
    fs::write(temp_dir.join(".zmetadata"), consolidated_metadata)?;

    Ok(())
}

#[test]
fn test_cli_with_hierarchical_store() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let store_path = temp_dir.path();

    // Create sample store
    create_sample_store(store_path).expect("Failed to create sample store");

    // Run the binary
    let output = Command::new(env!("CARGO_BIN_EXE_zarr-dump"))
        .arg(store_path.to_str().unwrap())
        .arg("--no-color")
        .output()
        .expect("Failed to execute zarr-dump");

    // Check that the command succeeded
    assert!(
        output.status.success(),
        "Command failed with status: {:?}\nStderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Verify key output components (allowing whitespace variance)
    let lines: Vec<&str> = stdout.lines().collect();

    // Check for NetCDF-style header
    assert!(
        lines
            .iter()
            .any(|line| line.contains("zarr") && line.contains("store")),
        "Missing zarr store header"
    );

    // Check for dimensions section
    assert!(
        lines.iter().any(|line| line.trim() == "dimensions:"),
        "Missing dimensions section"
    );

    // Check for specific dimensions
    assert!(
        lines
            .iter()
            .any(|line| line.contains("time") && line.contains("365")),
        "Missing time dimension"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.contains("lat") && line.contains("180")),
        "Missing lat dimension"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.contains("lon") && line.contains("360")),
        "Missing lon dimension"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.contains("level") && line.contains("50")),
        "Missing level dimension"
    );

    // Check for unlimited dimension
    assert!(
        lines
            .iter()
            .any(|line| line.contains("record") && line.contains("UNLIMITED")),
        "Missing unlimited dimension"
    );

    // Check for variables section
    assert!(
        lines.iter().any(|line| line.trim() == "variables:"),
        "Missing variables section"
    );

    // Check for specific variables
    assert!(
        lines
            .iter()
            .any(|line| line.contains("temperature(time, lat, lon)")),
        "Missing temperature variable"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.contains("pressure(time, level, lat, lon)")),
        "Missing pressure variable"
    );

    // Check for global attributes
    assert!(
        lines.iter().any(|line| line.contains("global attributes")),
        "Missing global attributes section"
    );
    assert!(
        lines.iter().any(|line| line.contains("title")),
        "Missing title attribute"
    );
}

#[test]
fn test_cli_with_consolidated_store() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let store_path = temp_dir.path();

    // Create consolidated store
    create_consolidated_store(store_path).expect("Failed to create consolidated store");

    // Run the binary
    let output = Command::new(env!("CARGO_BIN_EXE_zarr-dump"))
        .arg(store_path.to_str().unwrap())
        .arg("--no-color")
        .output()
        .expect("Failed to execute zarr-dump");

    // Check that the command succeeded
    assert!(
        output.status.success(),
        "Command failed with status: {:?}\nStderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Check for consolidated metadata loading message
    assert!(
        stdout.contains("Loaded consolidated metadata"),
        "Should indicate consolidated metadata was loaded"
    );

    // Check basic structure
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(
        lines.iter().any(|line| line.contains("zarr store")),
        "Missing zarr header"
    );
    assert!(
        lines.iter().any(|line| line.contains("dimensions:")),
        "Missing dimensions section"
    );
    assert!(
        lines.iter().any(|line| line.contains("variables:")),
        "Missing variables section"
    );
}

#[test]
fn test_cli_cf_check() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let store_path = temp_dir.path();

    create_sample_store(store_path).expect("Failed to create sample store");

    let output = Command::new(env!("CARGO_BIN_EXE_zarr-dump"))
        .arg("cf-check")
        .arg(store_path.to_str().unwrap())
        .output()
        .expect("Failed to execute zarr-dump cf-check");

    assert!(
        output.status.success(),
        "cf-check failed with status: {:?}\nStderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("cf-check"), "Missing cf-check header");
    assert!(stdout.contains("Summary:"), "Missing cf-check summary");
}

#[test]
fn test_cli_with_nonexistent_path() {
    let output = Command::new(env!("CARGO_BIN_EXE_zarr-dump"))
        .arg("/nonexistent/path")
        .output()
        .expect("Failed to execute zarr-dump");

    // Should fail with non-zero exit code
    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("does not exist"),
        "Should report that path doesn't exist"
    );
}

#[test]
fn test_cli_with_invalid_zarr_store() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let store_path = temp_dir.path();

    // Create an empty directory (not a valid Zarr store)
    // No .zarray, .zgroup, or .zmetadata files

    let output = Command::new(env!("CARGO_BIN_EXE_zarr-dump"))
        .arg(store_path.to_str().unwrap())
        .output()
        .expect("Failed to execute zarr-dump");

    // Should fail with non-zero exit code
    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("No Zarr arrays or groups found"),
        "Should report invalid Zarr store"
    );
}

#[test]
fn test_cli_color_output() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let store_path = temp_dir.path();

    create_sample_store(store_path).expect("Failed to create sample store");

    // Run without --no-color flag
    let output = Command::new(env!("CARGO_BIN_EXE_zarr-dump"))
        .arg(store_path.to_str().unwrap())
        .output()
        .expect("Failed to execute zarr-dump");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should contain ANSI escape codes for colors
    assert!(
        stdout.contains("\x1b["),
        "Should contain ANSI color codes when color is enabled"
    );
}

#[test]
fn test_cli_no_color_output() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let store_path = temp_dir.path();

    create_sample_store(store_path).expect("Failed to create sample store");

    // Run with --no-color flag
    let output = Command::new(env!("CARGO_BIN_EXE_zarr-dump"))
        .arg(store_path.to_str().unwrap())
        .arg("--no-color")
        .output()
        .expect("Failed to execute zarr-dump");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should not contain ANSI escape codes
    assert!(
        !stdout.contains("\x1b["),
        "Should not contain ANSI color codes when --no-color is used"
    );
}

#[test]
fn test_cli_variable_attributes() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let store_path = temp_dir.path();

    create_sample_store(store_path).expect("Failed to create sample store");

    let output = Command::new(env!("CARGO_BIN_EXE_zarr-dump"))
        .arg(store_path.to_str().unwrap())
        .arg("--no-color")
        .output()
        .expect("Failed to execute zarr-dump");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = stdout.lines().collect();

    // Check for variable attributes
    assert!(
        lines
            .iter()
            .any(|line| line.contains("temperature:long_name")),
        "Missing temperature long_name attribute"
    );
    assert!(
        lines.iter().any(|line| line.contains("pressure:units")),
        "Missing pressure units attribute"
    );
    assert!(
        lines.iter().any(|line| line.contains("degrees_C")),
        "Missing temperature units value"
    );
}

#[test]
fn test_dimension_inference_integration() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let store_path = temp_dir.path();

    // Create a more complex store to test dimension inference
    let zgroup_content = r#"{"zarr_format": 2}"#;
    fs::write(store_path.join(".zgroup"), zgroup_content).expect("Failed to write .zgroup");

    // Create two variables sharing a dimension with different sizes
    fs::create_dir_all(store_path.join("var1")).expect("Failed to create var1 dir");
    let var1_zarray = r#"{
        "zarr_format": 2,
        "shape": [100, 50],
        "chunks": [10, 50],
        "dtype": "<f4",
        "compressor": null,
        "fill_value": null,
        "order": "C",
        "filters": null
    }"#;
    fs::write(store_path.join("var1").join(".zarray"), var1_zarray)
        .expect("Failed to write var1 .zarray");

    let var1_zattrs = r#"{
        "_ARRAY_DIMENSIONS": ["time", "x"]
    }"#;
    fs::write(store_path.join("var1").join(".zattrs"), var1_zattrs)
        .expect("Failed to write var1 .zattrs");

    fs::create_dir_all(store_path.join("var2")).expect("Failed to create var2 dir");
    let var2_zarray = r#"{
        "zarr_format": 2,
        "shape": [200, 50],
        "chunks": [20, 50],
        "dtype": "<f4",
        "compressor": null,
        "fill_value": null,
        "order": "C", 
        "filters": null
    }"#;
    fs::write(store_path.join("var2").join(".zarray"), var2_zarray)
        .expect("Failed to write var2 .zarray");

    let var2_zattrs = r#"{
        "_ARRAY_DIMENSIONS": ["time", "x"]
    }"#;
    fs::write(store_path.join("var2").join(".zattrs"), var2_zattrs)
        .expect("Failed to write var2 .zattrs");

    let output = Command::new(env!("CARGO_BIN_EXE_zarr-dump"))
        .arg(store_path.to_str().unwrap())
        .arg("--no-color")
        .output()
        .expect("Failed to execute zarr-dump");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = stdout.lines().collect();

    // Time dimension should be detected as unlimited (different sizes: 100 vs 200)
    assert!(
        lines
            .iter()
            .any(|line| line.contains("time") && line.contains("UNLIMITED")),
        "Time dimension should be detected as unlimited"
    );

    // x dimension should not be unlimited (same size: 50)
    assert!(
        lines
            .iter()
            .any(|line| line.contains("x = 50") && !line.contains("UNLIMITED")),
        "x dimension should not be unlimited"
    );
}
