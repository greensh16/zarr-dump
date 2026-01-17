use crate::metadata::*;
use anyhow::{Context, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

pub struct ZarrStore {
    path: PathBuf,
}

impl ZarrStore {
    /// Create a new ZarrStore from a directory path
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        if !path.exists() {
            return Err(anyhow::anyhow!("Path does not exist: {}", path.display()));
        }

        if !path.is_dir() {
            return Err(anyhow::anyhow!(
                "Path is not a directory: {}",
                path.display()
            ));
        }

        Ok(Self { path })
    }

    /// Load metadata from the Zarr store, attempting consolidated read first
    pub async fn load_metadata(&self) -> Result<ZarrMetadata> {
        // First check if this is a Zarr v3 store by looking for zarr.json
        let zarr_json_path = self.path.join("zarr.json");
        if zarr_json_path.exists() {
            return self.load_v3_metadata().await;
        }

        // Try consolidated metadata first (v2)
        match self.load_consolidated_metadata().await {
            Ok(metadata) => {
                println!("Loaded consolidated metadata from .zmetadata");
                Ok(metadata)
            }
            Err(e) => {
                println!("Consolidated metadata not found: {}", e);
                println!("Falling back to hierarchical scanning...");
                self.load_hierarchical_metadata().await
            }
        }
    }

    /// Attempt to load consolidated metadata from .zmetadata file
    async fn load_consolidated_metadata(&self) -> Result<ZarrMetadata> {
        let zmetadata_path = self.path.join(".zmetadata");

        // Read .zmetadata file
        let data = fs::read(&zmetadata_path)
            .with_context(|| format!(
                "No consolidated metadata found at '{}'. This file is created when using zarr.convenience.consolidate_metadata().",
                zmetadata_path.display()
            ))?;

        let consolidated: ConsolidatedMetadata = serde_json::from_slice(&data)
            .with_context(|| format!(
                "Invalid consolidated metadata JSON format at '{}'. The file exists but contains malformed JSON.",
                zmetadata_path.display()
            ))?;

        // Parse consolidated metadata
        self.parse_consolidated_metadata(consolidated)
            .await
            .with_context(|| "Failed to process consolidated metadata entries")
    }

    /// Load metadata using hierarchical scanning of .zarray/.zattrs/.zgroup files
    async fn load_hierarchical_metadata(&self) -> Result<ZarrMetadata> {
        let mut metadata = ZarrMetadata::new();

        // Start from root and recursively scan
        Self::scan_directory(&mut metadata, "", &self.path)?;

        // Load async metadata for discovered items
        for (path, _) in metadata.variables.clone() {
            self.load_array_metadata(&mut metadata, &path).await?;
        }

        for (path, _) in metadata.groups.clone() {
            if !path.is_empty() {
                self.load_group_metadata(&mut metadata, &path).await?;
            }
        }

        // Load global attributes for root
        self.load_global_attributes(&mut metadata).await?;

        // Validate that we found a valid Zarr store
        if metadata.variables.is_empty() && metadata.groups.is_empty() {
            return Err(anyhow::anyhow!(
                "No Zarr arrays or groups found in '{}'. The directory must contain .zarray, .zgroup, or .zmetadata files to be a valid Zarr store.",
                self.path.display()
            ));
        }

        // Step 4: Infer dimensions and detect unlimited dimensions
        metadata.infer_dimensions();

        Ok(metadata)
    }

    /// Recursively scan directory for Zarr metadata files
    fn scan_directory(
        metadata: &mut ZarrMetadata,
        current_path: &str,
        fs_path: &Path,
    ) -> Result<()> {
        let entries = std::fs::read_dir(fs_path)
            .context(format!("Failed to read directory: {}", fs_path.display()))?;

        let mut has_zgroup = false;
        let mut has_zarray = false;
        let mut children = Vec::new();

        // First pass: check what files exist in this directory
        for entry in entries.flatten() {
            let filename = entry.file_name().to_string_lossy().to_string();
            let entry_path = entry.path();

            match filename.as_str() {
                ".zgroup" => has_zgroup = true,
                ".zarray" => has_zarray = true,
                name if !name.starts_with('.') && entry_path.is_dir() => {
                    children.push(name.to_string());
                }
                _ => {}
            }
        }

        // Mark items for later processing
        if has_zarray {
            // This is an array - add a placeholder
            let variable_name = if current_path.is_empty() {
                "root".to_string()
            } else {
                current_path
                    .split('/')
                    .next_back()
                    .unwrap_or(current_path)
                    .to_string()
            };

            let placeholder = Variable {
                name: variable_name,
                path: current_path.to_string(),
                dtype: "unknown".to_string(),
                shape: vec![],
                chunks: vec![],
                compressor: None,
                fill_value: None,
                order: "C".to_string(),
                filters: vec![],
                attributes: HashMap::new(),
                dimensions: vec![],
            };
            metadata
                .variables
                .insert(current_path.to_string(), placeholder);
        } else if has_zgroup {
            // This is a group - add a placeholder
            let group_name = if current_path.is_empty() {
                "/".to_string()
            } else {
                current_path
                    .split('/')
                    .next_back()
                    .unwrap_or(current_path)
                    .to_string()
            };

            let placeholder = Group {
                name: group_name,
                path: current_path.to_string(),
                attributes: HashMap::new(),
                children: Vec::new(),
            };

            if current_path.is_empty() {
                metadata.root_group = placeholder;
            } else {
                metadata
                    .groups
                    .insert(current_path.to_string(), placeholder);
            }
        }

        // Recursively scan subdirectories
        for child in children {
            let child_path = if current_path.is_empty() {
                child.clone()
            } else {
                format!("{}/{}", current_path, child)
            };
            let child_fs_path = fs_path.join(&child);

            Self::scan_directory(metadata, &child_path, &child_fs_path)?;
        }

        Ok(())
    }

    /// Load array metadata from .zarray and .zattrs files
    async fn load_array_metadata(&self, metadata: &mut ZarrMetadata, path: &str) -> Result<()> {
        let zarray_path = if path.is_empty() {
            self.path.join(".zarray")
        } else {
            self.path.join(path).join(".zarray")
        };

        let zattrs_path = if path.is_empty() {
            self.path.join(".zattrs")
        } else {
            self.path.join(path).join(".zattrs")
        };

        // Load .zarray
        let array_data = fs::read(&zarray_path)
            .with_context(|| {
                if path.is_empty() {
                    format!("Missing .zarray file for root variable at '{}'. This file is required to define array metadata (shape, dtype, chunks).", zarray_path.display())
                } else {
                    format!("Missing .zarray file for variable '{}' at '{}'. This file is required to define array metadata (shape, dtype, chunks).", path, zarray_path.display())
                }
            })?;

        let zarray: ZArrayMetadata = serde_json::from_slice(&array_data)
            .with_context(|| {
                if path.is_empty() {
                    format!("Invalid .zarray JSON format for root variable at '{}'. The file exists but contains malformed JSON.", zarray_path.display())
                } else {
                    format!("Invalid .zarray JSON format for variable '{}' at '{}'. The file exists but contains malformed JSON.", path, zarray_path.display())
                }
            })?;

        // Load .zattrs (optional)
        let attributes = match fs::read(&zattrs_path) {
            Ok(attrs_data) => {
                serde_json::from_slice::<HashMap<String, AttributeValue>>(&attrs_data)
                    .unwrap_or_default()
            }
            Err(_) => HashMap::new(),
        };

        // Create Variable struct
        let variable_name = if path.is_empty() {
            "root".to_string()
        } else {
            path.split('/').next_back().unwrap_or(path).to_string()
        };

        let dimensions = zarray
            .shape
            .iter()
            .enumerate()
            .map(|(i, &size)| Dimension {
                name: format!("dim_{}", i),
                size,
                is_unlimited: false, // Will be updated during dimension inference
            })
            .collect();

        let compressor = zarray
            .compressor
            .as_ref()
            .and_then(|c| c.get("id"))
            .and_then(|id| id.as_str())
            .map(|s| s.to_string());

        let filters = zarray
            .filters
            .as_ref()
            .map(|f| {
                f.iter()
                    .filter_map(|filter| {
                        filter
                            .get("id")
                            .and_then(|id| id.as_str().map(|s| s.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let fill_value = zarray
            .fill_value
            .as_ref()
            .map(|fv| serde_json::from_value(fv.clone()).unwrap_or(AttributeValue::Null));

        let variable = Variable {
            name: variable_name.clone(),
            path: path.to_string(),
            dtype: zarray.dtype,
            shape: zarray.shape,
            chunks: zarray.chunks,
            compressor,
            fill_value,
            order: zarray.order,
            filters,
            attributes,
            dimensions,
        };

        metadata.variables.insert(path.to_string(), variable);
        Ok(())
    }

    /// Load group metadata from .zgroup and .zattrs files
    async fn load_group_metadata(&self, metadata: &mut ZarrMetadata, path: &str) -> Result<()> {
        let zgroup_path = if path.is_empty() {
            self.path.join(".zgroup")
        } else {
            self.path.join(path).join(".zgroup")
        };

        let zattrs_path = if path.is_empty() {
            self.path.join(".zattrs")
        } else {
            self.path.join(path).join(".zattrs")
        };

        // Load .zgroup
        let group_data = fs::read(&zgroup_path)
            .with_context(|| {
                if path.is_empty() {
                    format!("Missing .zgroup file for root group at '{}'. This file is required to define group metadata.", zgroup_path.display())
                } else {
                    format!("Missing .zgroup file for group '{}' at '{}'. This file is required to define group metadata.", path, zgroup_path.display())
                }
            })?;

        let _zgroup: ZGroupMetadata = serde_json::from_slice(&group_data)
            .with_context(|| {
                if path.is_empty() {
                    format!("Invalid .zgroup JSON format for root group at '{}'. The file exists but contains malformed JSON.", zgroup_path.display())
                } else {
                    format!("Invalid .zgroup JSON format for group '{}' at '{}'. The file exists but contains malformed JSON.", path, zgroup_path.display())
                }
            })?;

        // Load .zattrs (optional)
        let attributes = match fs::read(&zattrs_path) {
            Ok(attrs_data) => {
                serde_json::from_slice::<HashMap<String, AttributeValue>>(&attrs_data)
                    .unwrap_or_default()
            }
            Err(_) => HashMap::new(),
        };

        // Create Group struct
        let group_name = if path.is_empty() {
            "/".to_string()
        } else {
            path.split('/').next_back().unwrap_or(path).to_string()
        };

        let group = Group {
            name: group_name,
            path: path.to_string(),
            attributes: attributes.clone(),
            children: Vec::new(), // Will be populated during directory scanning
        };

        if path.is_empty() {
            metadata.root_group = group;
            metadata.global_attributes = attributes;
        } else {
            metadata.groups.insert(path.to_string(), group);
        }

        Ok(())
    }

    /// Load global attributes from root .zattrs
    async fn load_global_attributes(&self, metadata: &mut ZarrMetadata) -> Result<()> {
        let zattrs_path = self.path.join(".zattrs");

        match fs::read(&zattrs_path) {
            Ok(attrs_data) => {
                let attributes: HashMap<String, AttributeValue> =
                    serde_json::from_slice(&attrs_data).unwrap_or_default();

                metadata.global_attributes = attributes.clone();
                metadata.root_group.attributes = attributes;
            }
            Err(_) => {
                // .zattrs is optional
            }
        }
        Ok(())
    }

    /// Parse consolidated metadata into ZarrMetadata structure
    async fn parse_consolidated_metadata(
        &self,
        consolidated: ConsolidatedMetadata,
    ) -> Result<ZarrMetadata> {
        let mut metadata = ZarrMetadata::new();
        metadata.zarr_format = 2; // Consolidated format is typically v2

        let metadata_map = consolidated.metadata.clone();
        for (key, value) in consolidated.metadata {
            if key == ".zattrs" {
                // Root attributes
                let attributes: HashMap<String, AttributeValue> =
                    serde_json::from_value(value).unwrap_or_default();
                metadata.global_attributes = attributes.clone();
                metadata.root_group.attributes = attributes;
            } else if key == ".zgroup" {
                // Root group metadata
                continue; // Already handled in metadata initialization
            } else if key.ends_with("/.zarray") {
                // Array metadata
                let path = key.trim_end_matches("/.zarray");
                let zarray: ZArrayMetadata = serde_json::from_value(value)
                    .context(format!("Failed to parse .zarray for {}", path))?;

                // Look for corresponding .zattrs
                let attrs_key = format!("{}/.zattrs", path);
                let attributes = metadata_map
                    .get(&attrs_key)
                    .and_then(|v| serde_json::from_value(v.clone()).ok())
                    .unwrap_or_default();

                self.create_variable_from_zarray(&mut metadata, path, zarray, attributes)
                    .await?;
            } else if key.ends_with("/.zgroup") {
                // Group metadata
                let path = key.trim_end_matches("/.zgroup");
                if !path.is_empty() {
                    let attrs_key = format!("{}/.zattrs", path);
                    let attributes = metadata_map
                        .get(&attrs_key)
                        .and_then(|v| serde_json::from_value(v.clone()).ok())
                        .unwrap_or_default();

                    let group_name = path.split('/').next_back().unwrap_or(path).to_string();
                    let group = Group {
                        name: group_name,
                        path: path.to_string(),
                        attributes,
                        children: Vec::new(),
                    };

                    metadata.groups.insert(path.to_string(), group);
                }
            }
        }

        // Step 4: Infer dimensions and detect unlimited dimensions
        metadata.infer_dimensions();

        Ok(metadata)
    }

    /// Create a Variable from ZArrayMetadata
    async fn create_variable_from_zarray(
        &self,
        metadata: &mut ZarrMetadata,
        path: &str,
        zarray: ZArrayMetadata,
        attributes: HashMap<String, AttributeValue>,
    ) -> Result<()> {
        let variable_name = path.split('/').next_back().unwrap_or(path).to_string();

        let dimensions = zarray
            .shape
            .iter()
            .enumerate()
            .map(|(i, &size)| Dimension {
                name: format!("dim_{}", i),
                size,
                is_unlimited: false, // Will be updated during dimension inference
            })
            .collect();

        let compressor = zarray
            .compressor
            .as_ref()
            .and_then(|c| c.get("id"))
            .and_then(|id| id.as_str())
            .map(|s| s.to_string());

        let filters = zarray
            .filters
            .as_ref()
            .map(|f| {
                f.iter()
                    .filter_map(|filter| {
                        filter
                            .get("id")
                            .and_then(|id| id.as_str().map(|s| s.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let fill_value = zarray
            .fill_value
            .as_ref()
            .map(|fv| serde_json::from_value(fv.clone()).unwrap_or(AttributeValue::Null));

        let variable = Variable {
            name: variable_name,
            path: path.to_string(),
            dtype: zarray.dtype,
            shape: zarray.shape,
            chunks: zarray.chunks,
            compressor,
            fill_value,
            order: zarray.order,
            filters,
            attributes,
            dimensions,
        };

        metadata.variables.insert(path.to_string(), variable);
        Ok(())
    }

    /// Read coordinate data for a variable using zarrs crate for full Zarr compliance
    pub async fn read_coordinate_data(&self, variable: &Variable) -> Result<Vec<f64>> {
        // For simplicity, we'll only handle 1D coordinate variables
        if variable.dimensions.len() != 1 {
            return Err(anyhow::anyhow!(
                "Only 1D coordinate variables are supported"
            ));
        }

        self.read_zarr_array_data(variable).await
    }

    /// Read a subset of an array and return the values as `f64`.
    ///
    /// This is intended for plotting (a 2D slice is represented as an N-D subset where all
    /// non-plotted dimensions have length 1).
    pub fn read_array_subset_f64(
        &self,
        variable: &Variable,
        ranges: &[std::ops::Range<u64>],
    ) -> Result<Vec<f64>> {
        use std::sync::Arc;
        use zarrs::array::{Array, DataType};
        use zarrs::array_subset::ArraySubset;
        use zarrs::storage::store::FilesystemStore;

        let store = FilesystemStore::new(&self.path)
            .map_err(|e| anyhow::anyhow!("Failed to create zarrs FilesystemStore: {}", e))?;

        let array_path = if variable.path.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", variable.path)
        };

        let array = Array::open(Arc::new(store), &array_path)
            .map_err(|e| anyhow::anyhow!("Failed to open array '{}': {}", array_path, e))?;

        let subset = ArraySubset::new_with_ranges(ranges);
        if subset.dimensionality() != array.shape().len() {
            return Err(anyhow::anyhow!(
                "Invalid subset dimensionality {} for array '{}' (expected {}).",
                subset.dimensionality(),
                variable.name,
                array.shape().len()
            ));
        }

        let values = match array.data_type() {
            DataType::Float64 => array
                .retrieve_array_subset_elements::<f64>(&subset)
                .with_context(|| {
                    format!("Failed to read subset for '{}' as float64", array_path)
                })?,
            DataType::Float32 => array
                .retrieve_array_subset_elements::<f32>(&subset)
                .with_context(|| format!("Failed to read subset for '{}' as float32", array_path))?
                .into_iter()
                .map(f64::from)
                .collect(),
            DataType::Int8 => array
                .retrieve_array_subset_elements::<i8>(&subset)
                .with_context(|| format!("Failed to read subset for '{}' as int8", array_path))?
                .into_iter()
                .map(f64::from)
                .collect(),
            DataType::Int16 => array
                .retrieve_array_subset_elements::<i16>(&subset)
                .with_context(|| format!("Failed to read subset for '{}' as int16", array_path))?
                .into_iter()
                .map(f64::from)
                .collect(),
            DataType::Int32 => array
                .retrieve_array_subset_elements::<i32>(&subset)
                .with_context(|| format!("Failed to read subset for '{}' as int32", array_path))?
                .into_iter()
                .map(f64::from)
                .collect(),
            DataType::Int64 => array
                .retrieve_array_subset_elements::<i64>(&subset)
                .with_context(|| format!("Failed to read subset for '{}' as int64", array_path))?
                .into_iter()
                .map(|v| v as f64)
                .collect(),
            DataType::UInt8 => array
                .retrieve_array_subset_elements::<u8>(&subset)
                .with_context(|| format!("Failed to read subset for '{}' as uint8", array_path))?
                .into_iter()
                .map(f64::from)
                .collect(),
            DataType::UInt16 => array
                .retrieve_array_subset_elements::<u16>(&subset)
                .with_context(|| format!("Failed to read subset for '{}' as uint16", array_path))?
                .into_iter()
                .map(f64::from)
                .collect(),
            DataType::UInt32 => array
                .retrieve_array_subset_elements::<u32>(&subset)
                .with_context(|| format!("Failed to read subset for '{}' as uint32", array_path))?
                .into_iter()
                .map(|v| v as f64)
                .collect(),
            DataType::UInt64 => array
                .retrieve_array_subset_elements::<u64>(&subset)
                .with_context(|| format!("Failed to read subset for '{}' as uint64", array_path))?
                .into_iter()
                .map(|v| v as f64)
                .collect(),
            DataType::Bool => array
                .retrieve_array_subset_elements::<bool>(&subset)
                .with_context(|| format!("Failed to read subset for '{}' as bool", array_path))?
                .into_iter()
                .map(|v| if v { 1.0 } else { 0.0 })
                .collect(),
            other => {
                return Err(anyhow::anyhow!(
                    "Unsupported data type '{}' for plotting. Supported: bool, int8/16/32/64, uint8/16/32/64, float32/64.",
                    other.name()
                ));
            }
        };

        Ok(values)
    }

    /// Read array data using the zarrs crate with proper compression support
    async fn read_zarr_array_data(&self, variable: &Variable) -> Result<Vec<f64>> {
        // Try different zarrs API approaches
        self.try_zarrs_api_v1(variable)
            .await
            .or_else(|_| self.try_zarrs_api_v2(variable))
            .or_else(|_| self.fallback_to_manual_read(variable))
    }

    /// Try zarrs API approach 1: Using filesystem store
    async fn try_zarrs_api_v1(&self, variable: &Variable) -> Result<Vec<f64>> {
        use zarrs::array::Array;
        use zarrs::array_subset::ArraySubset;
        use zarrs::storage::store::FilesystemStore;

        let store = FilesystemStore::new(&self.path)
            .map_err(|e| anyhow::anyhow!("Failed to create zarrs FilesystemStore: {}", e))?;

        let array_path = if variable.path.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", variable.path)
        };

        let array = Array::open(std::sync::Arc::new(store), &array_path)
            .map_err(|e| anyhow::anyhow!("Failed to open array '{}': {}", array_path, e))?;

        // Create array subset for the entire array
        let shape = array.shape();
        let array_subset = ArraySubset::new_with_shape(shape.to_vec());

        // Read the entire array
        let array_bytes = array
            .retrieve_array_subset(&array_subset)
            .map_err(|e| anyhow::anyhow!("Failed to read array data: {}", e))?;

        // Convert ArrayBytes to Vec<f64>
        let data = self.convert_array_bytes_to_f64(&array_bytes, &variable.dtype)?;

        Ok(data)
    }

    /// Convert ArrayBytes to Vec<f64> based on dtype
    fn convert_array_bytes_to_f64(
        &self,
        array_bytes: &zarrs::array::ArrayBytes,
        dtype: &str,
    ) -> Result<Vec<f64>> {
        use byteorder::{LittleEndian, ReadBytesExt};

        // Extract bytes from ArrayBytes enum
        let bytes: &[u8] = match array_bytes {
            zarrs::array::ArrayBytes::Variable(data, _offsets) => data.as_ref(),
            zarrs::array::ArrayBytes::Fixed(data) => data.as_ref(),
        };

        let mut reader = std::io::Cursor::new(bytes);
        let mut data = Vec::new();

        match dtype {
            "<f8" => {
                // 64-bit little-endian float
                while reader.position() < bytes.len() as u64 {
                    match reader.read_f64::<LittleEndian>() {
                        Ok(val) => data.push(val),
                        Err(_) => break,
                    }
                }
            }
            "<f4" => {
                // 32-bit little-endian float
                while reader.position() < bytes.len() as u64 {
                    match reader.read_f32::<LittleEndian>() {
                        Ok(val) => data.push(val as f64),
                        Err(_) => break,
                    }
                }
            }
            "<i4" => {
                // 32-bit little-endian integer
                while reader.position() < bytes.len() as u64 {
                    match reader.read_i32::<LittleEndian>() {
                        Ok(val) => data.push(val as f64),
                        Err(_) => break,
                    }
                }
            }
            "<i8" => {
                // 64-bit little-endian integer
                while reader.position() < bytes.len() as u64 {
                    match reader.read_i64::<LittleEndian>() {
                        Ok(val) => data.push(val as f64),
                        Err(_) => break,
                    }
                }
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported dtype for coordinate data: {}",
                    dtype
                ));
            }
        }

        Ok(data)
    }

    /// Try zarrs API approach 2: Simplified approach
    fn try_zarrs_api_v2(&self, _variable: &Variable) -> Result<Vec<f64>> {
        // This approach is currently not working due to API limitations
        Err(anyhow::anyhow!(
            "Alternative zarrs API approach not available"
        ))
    }

    /// Fallback to manual reading for uncompressed data
    fn fallback_to_manual_read(&self, variable: &Variable) -> Result<Vec<f64>> {
        // Check if the variable has compression - if so, we can't handle it with this simple implementation
        if variable.compressor.is_some() {
            return Err(anyhow::anyhow!(
                "Variable '{}' uses compression ('{}'), which could not be handled by the zarrs crate. \
                This may be due to API version incompatibility or missing compression support.",
                variable.name,
                variable.compressor.as_ref().unwrap()
            ));
        }

        // Build the path to the first chunk (0)
        let chunk_path = if variable.path.is_empty() {
            self.path.join("0")
        } else {
            self.path.join(&variable.path).join("0")
        };

        // Check if the chunk file exists
        if !chunk_path.exists() {
            return Err(anyhow::anyhow!(
                "Chunk file not found: {}",
                chunk_path.display()
            ));
        }

        // Read the raw chunk data
        let mut file = File::open(&chunk_path)
            .with_context(|| format!("Failed to open chunk file: {}", chunk_path.display()))?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .with_context(|| format!("Failed to read chunk file: {}", chunk_path.display()))?;

        // Parse the data based on dtype
        self.parse_coordinate_data(&buffer, &variable.dtype, variable.shape[0] as usize)
    }

    /// Parse binary data based on dtype (for uncompressed data only)
    fn parse_coordinate_data(&self, buffer: &[u8], dtype: &str, size: usize) -> Result<Vec<f64>> {
        let mut reader = std::io::Cursor::new(buffer);
        let mut data = Vec::with_capacity(size);

        match dtype {
            "<f8" => {
                // 64-bit little-endian float
                for _ in 0..size {
                    match reader.read_f64::<LittleEndian>() {
                        Ok(val) => data.push(val),
                        Err(_) => break,
                    }
                }
            }
            "<f4" => {
                // 32-bit little-endian float
                for _ in 0..size {
                    match reader.read_f32::<LittleEndian>() {
                        Ok(val) => data.push(val as f64),
                        Err(_) => break,
                    }
                }
            }
            "<i4" => {
                // 32-bit little-endian integer
                for _ in 0..size {
                    match reader.read_i32::<LittleEndian>() {
                        Ok(val) => data.push(val as f64),
                        Err(_) => break,
                    }
                }
            }
            "<i8" => {
                // 64-bit little-endian integer
                for _ in 0..size {
                    match reader.read_i64::<LittleEndian>() {
                        Ok(val) => data.push(val as f64),
                        Err(_) => break,
                    }
                }
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported dtype for coordinate data: {}",
                    dtype
                ));
            }
        }

        Ok(data)
    }

    /// Load Zarr v3 metadata from zarr.json
    async fn load_v3_metadata(&self) -> Result<ZarrMetadata> {
        use crate::metadata::ZarrV3Root;

        let zarr_json_path = self.path.join("zarr.json");
        let data = fs::read(&zarr_json_path).with_context(|| {
            format!("Failed to read zarr.json at '{}'", zarr_json_path.display())
        })?;

        let v3_root: ZarrV3Root =
            serde_json::from_slice(&data).with_context(|| "Failed to parse zarr.json")?;

        let mut metadata = ZarrMetadata::new();
        metadata.zarr_format = 3;

        // Handle root attributes
        if let Some(attrs) = v3_root.attributes {
            metadata.global_attributes = attrs.clone();
            metadata.root_group.attributes = attrs;
        }

        // Check for consolidated metadata first
        if let Some(consolidated) = v3_root.consolidated_metadata {
            println!("Loaded consolidated metadata from zarr.json (Zarr v3)");
            for (name, v3_array) in consolidated.metadata {
                self.convert_v3_array_to_variable(&mut metadata, &name, v3_array)?;
            }
        } else {
            // Fall back to hierarchical scanning for v3
            println!("Scanning for individual zarr.json files (Zarr v3)...");
            self.scan_v3_directory(&mut metadata, "", &self.path)?;
        }

        // Infer dimensions
        metadata.infer_dimensions();

        Ok(metadata)
    }

    /// Recursively scan directory for Zarr v3 zarr.json files
    fn scan_v3_directory(
        &self,
        metadata: &mut ZarrMetadata,
        current_path: &str,
        fs_path: &Path,
    ) -> Result<()> {
        use crate::metadata::ZarrV3ArrayMetadata;

        let entries = std::fs::read_dir(fs_path)
            .context(format!("Failed to read directory: {}", fs_path.display()))?;

        for entry in entries.flatten() {
            let filename = entry.file_name().to_string_lossy().to_string();
            let entry_path = entry.path();

            // Skip hidden files and chunk data directories
            if filename.starts_with('.') || filename == "c" {
                continue;
            }

            if entry_path.is_dir() {
                let zarr_json = entry_path.join("zarr.json");
                if zarr_json.exists() {
                    // Read and parse the zarr.json
                    let data = fs::read(&zarr_json)?;
                    let v3_array: ZarrV3ArrayMetadata = serde_json::from_slice(&data).context(
                        format!("Failed to parse zarr.json at {}", zarr_json.display()),
                    )?;

                    let var_path = if current_path.is_empty() {
                        filename.clone()
                    } else {
                        format!("{}/{}", current_path, filename)
                    };

                    self.convert_v3_array_to_variable(metadata, &var_path, v3_array)?;
                }

                // Recursively scan subdirectories
                let child_path = if current_path.is_empty() {
                    filename
                } else {
                    format!("{}/{}", current_path, filename)
                };
                self.scan_v3_directory(metadata, &child_path, &entry_path)?;
            }
        }

        Ok(())
    }

    /// Convert Zarr v3 array metadata to internal Variable structure
    fn convert_v3_array_to_variable(
        &self,
        metadata: &mut ZarrMetadata,
        path: &str,
        v3_array: crate::metadata::ZarrV3ArrayMetadata,
    ) -> Result<()> {
        let variable_name = path.split('/').next_back().unwrap_or(path).to_string();

        // Convert v3 data_type to v2 dtype format
        let dtype = self.convert_v3_datatype_to_v2(&v3_array.data_type);

        // Extract chunks from chunk_grid
        let chunks = v3_array.chunk_grid.configuration.chunk_shape.clone();

        // Extract codec names
        let codecs: Vec<String> = v3_array.codecs.iter().map(|c| c.name.clone()).collect();

        // Determine primary compressor (skip 'bytes' codec which is just endianness)
        let compressor = codecs.iter().find(|c| *c != "bytes").cloned();

        // Prepare attributes - merge v3 attributes with dimension_names
        let mut attributes = v3_array.attributes.clone().unwrap_or_default();

        // Add dimension_names as an attribute for easier processing
        if let Some(ref dim_names) = v3_array.dimension_names {
            let dim_values: Vec<AttributeValue> = dim_names
                .iter()
                .map(|s| AttributeValue::String(s.clone()))
                .collect();
            attributes.insert(
                "dimension_names".to_string(),
                AttributeValue::Array(dim_values),
            );
        }

        let dimensions = v3_array
            .shape
            .iter()
            .enumerate()
            .map(|(i, &size)| Dimension {
                name: format!("dim_{}", i),
                size,
                is_unlimited: false,
            })
            .collect();

        let variable = Variable {
            name: variable_name,
            path: path.to_string(),
            dtype,
            shape: v3_array.shape,
            chunks,
            compressor,
            fill_value: Some(AttributeValue::String("NaN".to_string())), // Simplified
            order: "C".to_string(),
            filters: vec![],
            attributes,
            dimensions,
        };

        metadata.variables.insert(path.to_string(), variable);
        Ok(())
    }

    /// Convert Zarr v3 data_type to v2 dtype format
    fn convert_v3_datatype_to_v2(&self, data_type: &str) -> String {
        match data_type {
            "float32" => "<f4".to_string(),
            "float64" => "<f8".to_string(),
            "int8" => "<i1".to_string(),
            "int16" => "<i2".to_string(),
            "int32" => "<i4".to_string(),
            "int64" => "<i8".to_string(),
            "uint8" => "<u1".to_string(),
            "uint16" => "<u2".to_string(),
            "uint32" => "<u4".to_string(),
            "uint64" => "<u8".to_string(),
            "bool" => "?".to_string(),
            other => other.to_string(), // Pass through unknown types
        }
    }
}
