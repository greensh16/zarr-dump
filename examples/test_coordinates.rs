use anyhow::Result;
use zarr_dump::ZarrStore;

#[tokio::main]
async fn main() -> Result<()> {
    let zarr_path = "/Users/green/GitHub/zarr_summery/test_data/ua_Amon_CanESM5_ssp126_r1i1p1f1_gn_201501-210012.zarr";

    println!("Testing coordinate data reading with compression...");
    println!("Loading Zarr store: {}", zarr_path);

    // Create Zarr store
    let store = ZarrStore::new(zarr_path)?;

    // Load metadata
    let metadata = store.load_metadata().await?;

    // Test reading latitude coordinates (which should be compressed with Blosc/LZ4)
    if let Some(lat_var) = metadata.variables.get("lat") {
        println!("\n=== Testing lat coordinate (compressed with Blosc/LZ4) ===");
        println!("Variable info:");
        println!("  Name: {}", lat_var.name);
        println!("  Shape: {:?}", lat_var.shape);
        println!("  Dtype: {}", lat_var.dtype);
        println!("  Compressor: {:?}", lat_var.compressor);

        match store.read_coordinate_data(lat_var).await {
            Ok(data) => {
                println!("  Successfully read {} values:", data.len());
                if data.len() <= 10 {
                    println!("  Values: {:?}", data);
                } else {
                    println!("  First 5 values: {:?}", &data[..5]);
                    println!("  Last 5 values: {:?}", &data[data.len() - 5..]);
                }
                println!(
                    "  Min: {:.6}",
                    data.iter().fold(f64::INFINITY, |a, &b| a.min(b))
                );
                println!(
                    "  Max: {:.6}",
                    data.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b))
                );
            }
            Err(e) => {
                println!("  ERROR reading coordinate data: {}", e);
            }
        }
    } else {
        println!("No 'lat' variable found!");
    }

    // Test reading longitude coordinates
    if let Some(lon_var) = metadata.variables.get("lon") {
        println!("\n=== Testing lon coordinate (compressed with Blosc/LZ4) ===");
        println!("Variable info:");
        println!("  Name: {}", lon_var.name);
        println!("  Shape: {:?}", lon_var.shape);
        println!("  Dtype: {}", lon_var.dtype);
        println!("  Compressor: {:?}", lon_var.compressor);

        match store.read_coordinate_data(lon_var).await {
            Ok(data) => {
                println!("  Successfully read {} values:", data.len());
                if data.len() <= 10 {
                    println!("  Values: {:?}", data);
                } else {
                    println!("  First 5 values: {:?}", &data[..5]);
                    println!("  Last 5 values: {:?}", &data[data.len() - 5..]);
                }
                println!(
                    "  Min: {:.6}",
                    data.iter().fold(f64::INFINITY, |a, &b| a.min(b))
                );
                println!(
                    "  Max: {:.6}",
                    data.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b))
                );
            }
            Err(e) => {
                println!("  ERROR reading coordinate data: {}", e);
            }
        }
    } else {
        println!("No 'lon' variable found!");
    }

    // Test reading pressure level coordinates
    if let Some(plev_var) = metadata.variables.get("plev") {
        println!("\n=== Testing plev coordinate (compressed with Blosc/LZ4) ===");
        println!("Variable info:");
        println!("  Name: {}", plev_var.name);
        println!("  Shape: {:?}", plev_var.shape);
        println!("  Dtype: {}", plev_var.dtype);
        println!("  Compressor: {:?}", plev_var.compressor);

        match store.read_coordinate_data(plev_var).await {
            Ok(data) => {
                println!("  Successfully read {} values:", data.len());
                if data.len() <= 10 {
                    println!("  Values: {:?}", data);
                } else {
                    println!("  First 5 values: {:?}", &data[..5]);
                    println!("  Last 5 values: {:?}", &data[data.len() - 5..]);
                }
                println!(
                    "  Min: {:.6}",
                    data.iter().fold(f64::INFINITY, |a, &b| a.min(b))
                );
                println!(
                    "  Max: {:.6}",
                    data.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b))
                );
            }
            Err(e) => {
                println!("  ERROR reading coordinate data: {}", e);
            }
        }
    } else {
        println!("No 'plev' variable found!");
    }

    // Test reading time coordinates
    if let Some(time_var) = metadata.variables.get("time") {
        println!("\n=== Testing time coordinate (compressed with Blosc/LZ4) ===");
        println!("Variable info:");
        println!("  Name: {}", time_var.name);
        println!("  Shape: {:?}", time_var.shape);
        println!("  Dtype: {}", time_var.dtype);
        println!("  Compressor: {:?}", time_var.compressor);

        match store.read_coordinate_data(time_var).await {
            Ok(data) => {
                println!("  Successfully read {} values:", data.len());
                if data.len() <= 10 {
                    println!("  Values: {:?}", data);
                } else {
                    println!("  First 5 values: {:?}", &data[..5]);
                    println!("  Last 5 values: {:?}", &data[data.len() - 5..]);
                }
                println!(
                    "  Min: {:.6}",
                    data.iter().fold(f64::INFINITY, |a, &b| a.min(b))
                );
                println!(
                    "  Max: {:.6}",
                    data.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b))
                );
            }
            Err(e) => {
                println!("  ERROR reading coordinate data: {}", e);
            }
        }
    } else {
        println!("No 'time' variable found!");
    }

    println!("\n=== Coordinate reading test completed ===");

    Ok(())
}
