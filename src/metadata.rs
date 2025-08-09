use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a dimension in a Zarr array
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dimension {
    pub name: String,
    pub size: u64,
    pub is_unlimited: bool,
}

/// Represents an attribute in Zarr metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AttributeValue {
    String(String),
    Number(f64),
    Integer(i64),
    Boolean(bool),
    Array(Vec<AttributeValue>),
    Object(HashMap<String, AttributeValue>),
    Null,
}

/// Represents a single attribute
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attribute {
    pub name: String,
    pub value: AttributeValue,
}

/// Represents a Zarr variable/array
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Variable {
    pub name: String,
    pub path: String,
    pub dtype: String,
    pub shape: Vec<u64>,
    pub chunks: Vec<u64>,
    pub compressor: Option<String>,
    pub fill_value: Option<AttributeValue>,
    pub order: String,
    pub filters: Vec<String>,
    pub attributes: HashMap<String, AttributeValue>,
    pub dimensions: Vec<Dimension>,
}

/// Represents a Zarr group
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Group {
    pub name: String,
    pub path: String,
    pub attributes: HashMap<String, AttributeValue>,
    pub children: Vec<String>, // Child group/array names
}

/// Dimension information across the entire store
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct DimensionInfo {
    pub name: String,
    pub max_length: u64,
    pub is_unlimited: bool,
    pub appearances: Vec<(String, u64)>, // (variable_path, size) pairs
}

/// Root metadata structure for a Zarr store
#[derive(Debug)]
pub struct ZarrMetadata {
    pub zarr_format: u8,
    pub global_attributes: HashMap<String, AttributeValue>,
    pub groups: HashMap<String, Group>,
    pub variables: HashMap<String, Variable>,
    pub root_group: Group,
    pub dimensions: HashMap<String, DimensionInfo>,
}

impl Default for ZarrMetadata {
    fn default() -> Self {
        Self {
            zarr_format: 2,
            global_attributes: HashMap::new(),
            variables: HashMap::new(),
            groups: HashMap::new(),
            dimensions: HashMap::new(),
            root_group: Group {
                name: "/".to_string(),
                path: "/".to_string(),
                attributes: HashMap::new(),
                children: Vec::new(),
            },
        }
    }
}

impl ZarrMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    /// Infer dimensions and detect unlimited dimensions from all variables
    pub fn infer_dimensions(&mut self) {
        let mut dimension_map: HashMap<String, Vec<(String, u64)>> = HashMap::new();

        // Collect dimension information from all variables
        for (var_path, variable) in &self.variables {
            let dim_names = self.extract_dimension_names(variable);

            for (i, &size) in variable.shape.iter().enumerate() {
                let dim_name = dim_names
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| format!("dim_{}", i));

                dimension_map
                    .entry(dim_name.clone())
                    .or_default()
                    .push((var_path.clone(), size));
            }
        }

        // Build DimensionInfo for each dimension
        for (dim_name, appearances) in dimension_map {
            let max_length = appearances.iter().map(|(_, size)| *size).max().unwrap_or(0);

            // Check if dimension is unlimited:
            // 1. Appears in multiple variables with different sizes
            // 2. OR any appearance has size 0 (future-proofing for zarr v3)
            let mut unique_sizes: std::collections::HashSet<u64> = std::collections::HashSet::new();
            let mut has_zero_size = false;

            for (_, size) in &appearances {
                unique_sizes.insert(*size);
                if *size == 0 {
                    has_zero_size = true;
                }
            }

            let is_unlimited = has_zero_size || (appearances.len() > 1 && unique_sizes.len() > 1);

            let dim_info = DimensionInfo {
                name: dim_name.clone(),
                max_length,
                is_unlimited,
                appearances,
            };

            self.dimensions.insert(dim_name, dim_info);
        }

        // Update dimension information in variables
        let variable_paths: Vec<String> = self.variables.keys().cloned().collect();
        for path in variable_paths {
            let dim_names = {
                let variable = &self.variables[&path];
                self.extract_dimension_names(variable)
            };

            let variable = self.variables.get_mut(&path).unwrap();
            variable.dimensions = variable
                .shape
                .iter()
                .enumerate()
                .map(|(i, &size)| {
                    let dim_name = dim_names
                        .get(i)
                        .cloned()
                        .unwrap_or_else(|| format!("dim_{}", i));

                    let is_unlimited = self
                        .dimensions
                        .get(&dim_name)
                        .map(|info| info.is_unlimited)
                        .unwrap_or(false);

                    Dimension {
                        name: dim_name,
                        size,
                        is_unlimited,
                    }
                })
                .collect();
        }
    }

    /// Extract dimension names from _ARRAY_DIMENSIONS attribute or generate defaults
    pub fn extract_dimension_names(&self, variable: &Variable) -> Vec<String> {
        // Look for _ARRAY_DIMENSIONS in variable attributes
        if let Some(AttributeValue::Array(dims)) = variable.attributes.get("_ARRAY_DIMENSIONS") {
            dims.iter()
                .filter_map(|val| match val {
                    AttributeValue::String(s) => Some(s.clone()),
                    _ => None,
                })
                .collect()
        } else {
            // Generate default dimension names
            (0..variable.shape.len())
                .map(|i| format!("dim_{}", i))
                .collect()
        }
    }
}

/// Raw Zarr array metadata from .zarray file
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct ZArrayMetadata {
    pub zarr_format: u8,
    pub shape: Vec<u64>,
    pub chunks: Vec<u64>,
    pub dtype: String,
    pub compressor: Option<serde_json::Value>,
    pub fill_value: Option<serde_json::Value>,
    pub order: String,
    pub filters: Option<Vec<serde_json::Value>>,
}

/// Raw Zarr group metadata from .zgroup file
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct ZGroupMetadata {
    pub zarr_format: u8,
}

/// Consolidated metadata from .zmetadata file
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct ConsolidatedMetadata {
    pub zarr_consolidated_format: u8,
    pub metadata: HashMap<String, serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_zarr_metadata_new() {
        let metadata = ZarrMetadata::new();

        assert_eq!(metadata.zarr_format, 2);
        assert!(metadata.global_attributes.is_empty());
        assert!(metadata.groups.is_empty());
        assert!(metadata.variables.is_empty());
        assert!(metadata.dimensions.is_empty());
        assert_eq!(metadata.root_group.name, "/");
        assert_eq!(metadata.root_group.path, "/");
    }

    #[test]
    fn test_extract_dimension_names_with_array_dimensions() {
        let metadata = ZarrMetadata::new();
        let mut attributes = HashMap::new();

        // Test with _ARRAY_DIMENSIONS attribute
        let dims = vec![
            AttributeValue::String("time".to_string()),
            AttributeValue::String("lat".to_string()),
            AttributeValue::String("lon".to_string()),
        ];
        attributes.insert("_ARRAY_DIMENSIONS".to_string(), AttributeValue::Array(dims));

        let variable = Variable {
            name: "temperature".to_string(),
            path: "temperature".to_string(),
            dtype: "float64".to_string(),
            shape: vec![10, 20, 30],
            chunks: vec![5, 10, 15],
            compressor: None,
            fill_value: None,
            order: "C".to_string(),
            filters: vec![],
            attributes,
            dimensions: vec![],
        };

        let dim_names = metadata.extract_dimension_names(&variable);
        assert_eq!(dim_names, vec!["time", "lat", "lon"]);
    }

    #[test]
    fn test_extract_dimension_names_default() {
        let metadata = ZarrMetadata::new();
        let variable = Variable {
            name: "data".to_string(),
            path: "data".to_string(),
            dtype: "float32".to_string(),
            shape: vec![100, 200],
            chunks: vec![50, 100],
            compressor: None,
            fill_value: None,
            order: "C".to_string(),
            filters: vec![],
            attributes: HashMap::new(),
            dimensions: vec![],
        };

        let dim_names = metadata.extract_dimension_names(&variable);
        assert_eq!(dim_names, vec!["dim_0", "dim_1"]);
    }

    #[test]
    fn test_infer_dimensions_single_variable() {
        let mut metadata = ZarrMetadata::new();

        let variable = Variable {
            name: "temp".to_string(),
            path: "temp".to_string(),
            dtype: "float64".to_string(),
            shape: vec![365, 180, 360],
            chunks: vec![1, 180, 360],
            compressor: None,
            fill_value: None,
            order: "C".to_string(),
            filters: vec![],
            attributes: HashMap::new(),
            dimensions: vec![],
        };

        metadata.variables.insert("temp".to_string(), variable);
        metadata.infer_dimensions();

        assert_eq!(metadata.dimensions.len(), 3);

        let dim_0 = metadata.dimensions.get("dim_0").unwrap();
        assert_eq!(dim_0.max_length, 365);
        assert!(!dim_0.is_unlimited);
        assert_eq!(dim_0.appearances.len(), 1);

        let dim_1 = metadata.dimensions.get("dim_1").unwrap();
        assert_eq!(dim_1.max_length, 180);
        assert!(!dim_1.is_unlimited);

        let dim_2 = metadata.dimensions.get("dim_2").unwrap();
        assert_eq!(dim_2.max_length, 360);
        assert!(!dim_2.is_unlimited);

        // Check that variable dimensions were updated
        let variable = metadata.variables.get("temp").unwrap();
        assert_eq!(variable.dimensions.len(), 3);
        assert_eq!(variable.dimensions[0].name, "dim_0");
        assert_eq!(variable.dimensions[0].size, 365);
        assert!(!variable.dimensions[0].is_unlimited);
    }

    #[test]
    fn test_infer_dimensions_unlimited_detection() {
        let mut metadata = ZarrMetadata::new();

        // Create two variables with same dimension name but different sizes
        let mut attrs1 = HashMap::new();
        attrs1.insert(
            "_ARRAY_DIMENSIONS".to_string(),
            AttributeValue::Array(vec![AttributeValue::String("time".to_string())]),
        );

        let mut attrs2 = HashMap::new();
        attrs2.insert(
            "_ARRAY_DIMENSIONS".to_string(),
            AttributeValue::Array(vec![AttributeValue::String("time".to_string())]),
        );

        let var1 = Variable {
            name: "temp1".to_string(),
            path: "temp1".to_string(),
            dtype: "float64".to_string(),
            shape: vec![100],
            chunks: vec![10],
            compressor: None,
            fill_value: None,
            order: "C".to_string(),
            filters: vec![],
            attributes: attrs1,
            dimensions: vec![],
        };

        let var2 = Variable {
            name: "temp2".to_string(),
            path: "temp2".to_string(),
            dtype: "float64".to_string(),
            shape: vec![200], // Different size for same dimension
            chunks: vec![20],
            compressor: None,
            fill_value: None,
            order: "C".to_string(),
            filters: vec![],
            attributes: attrs2,
            dimensions: vec![],
        };

        metadata.variables.insert("temp1".to_string(), var1);
        metadata.variables.insert("temp2".to_string(), var2);
        metadata.infer_dimensions();

        assert_eq!(metadata.dimensions.len(), 1);

        let time_dim = metadata.dimensions.get("time").unwrap();
        assert_eq!(time_dim.max_length, 200);
        assert!(time_dim.is_unlimited); // Should be detected as unlimited
        assert_eq!(time_dim.appearances.len(), 2);

        // Check that variables were updated with unlimited flag
        let var1 = metadata.variables.get("temp1").unwrap();
        assert!(var1.dimensions[0].is_unlimited);

        let var2 = metadata.variables.get("temp2").unwrap();
        assert!(var2.dimensions[0].is_unlimited);
    }

    #[test]
    fn test_infer_dimensions_zero_size_unlimited() {
        let mut metadata = ZarrMetadata::new();

        let variable = Variable {
            name: "unlimited_var".to_string(),
            path: "unlimited_var".to_string(),
            dtype: "float64".to_string(),
            shape: vec![0, 100], // Zero size in first dimension
            chunks: vec![1, 100],
            compressor: None,
            fill_value: None,
            order: "C".to_string(),
            filters: vec![],
            attributes: HashMap::new(),
            dimensions: vec![],
        };

        metadata
            .variables
            .insert("unlimited_var".to_string(), variable);
        metadata.infer_dimensions();

        let dim_0 = metadata.dimensions.get("dim_0").unwrap();
        assert!(dim_0.is_unlimited); // Should be unlimited due to zero size
        assert_eq!(dim_0.max_length, 0);

        let dim_1 = metadata.dimensions.get("dim_1").unwrap();
        assert!(!dim_1.is_unlimited); // Regular dimension
        assert_eq!(dim_1.max_length, 100);
    }

    #[test]
    fn test_attribute_value_serialization() {
        // Test different attribute value types
        let string_val = AttributeValue::String("test".to_string());
        let number_val = AttributeValue::Number(3.14);
        let int_val = AttributeValue::Integer(42);
        let bool_val = AttributeValue::Boolean(true);
        let null_val = AttributeValue::Null;

        // Test array
        let array_val = AttributeValue::Array(vec![
            AttributeValue::Integer(1),
            AttributeValue::Integer(2),
            AttributeValue::Integer(3),
        ]);

        // Test object
        let mut obj_map = HashMap::new();
        obj_map.insert(
            "key".to_string(),
            AttributeValue::String("value".to_string()),
        );
        let obj_val = AttributeValue::Object(obj_map);

        // These should not panic when serialized/deserialized
        let json_str = serde_json::to_string(&string_val).unwrap();
        assert!(json_str.contains("test"));

        let json_str = serde_json::to_string(&array_val).unwrap();
        assert!(json_str.contains("1"));

        let json_str = serde_json::to_string(&obj_val).unwrap();
        assert!(json_str.contains("key"));
    }

    #[test]
    fn test_zarr_array_metadata_parsing() {
        let json_data = r#"{
            "zarr_format": 2,
            "shape": [100, 200],
            "chunks": [10, 20],
            "dtype": "<f8",
            "compressor": {"id": "zstd", "level": 3},
            "fill_value": null,
            "order": "C",
            "filters": [{"id": "shuffle"}]
        }"#;

        let result: Result<ZArrayMetadata, _> = serde_json::from_str(json_data);
        assert!(result.is_ok());

        let metadata = result.unwrap();
        assert_eq!(metadata.zarr_format, 2);
        assert_eq!(metadata.shape, vec![100, 200]);
        assert_eq!(metadata.chunks, vec![10, 20]);
        assert_eq!(metadata.dtype, "<f8");
        assert_eq!(metadata.order, "C");
        assert!(metadata.compressor.is_some());
        assert!(metadata.filters.is_some());
    }

    #[test]
    fn test_zarr_group_metadata_parsing() {
        let json_data = r#"{
            "zarr_format": 2
        }"#;

        let result: Result<ZGroupMetadata, _> = serde_json::from_str(json_data);
        assert!(result.is_ok());

        let metadata = result.unwrap();
        assert_eq!(metadata.zarr_format, 2);
    }

    #[test]
    fn test_infer_dimensions_complex_scenario() {
        let mut metadata = ZarrMetadata::new();

        // Create variables with overlapping dimensions
        let mut attrs1 = HashMap::new();
        attrs1.insert(
            "_ARRAY_DIMENSIONS".to_string(),
            AttributeValue::Array(vec![
                AttributeValue::String("time".to_string()),
                AttributeValue::String("lat".to_string()),
                AttributeValue::String("lon".to_string()),
            ]),
        );

        let mut attrs2 = HashMap::new();
        attrs2.insert(
            "_ARRAY_DIMENSIONS".to_string(),
            AttributeValue::Array(vec![
                AttributeValue::String("time".to_string()),
                AttributeValue::String("level".to_string()),
            ]),
        );

        let var1 = Variable {
            name: "temperature".to_string(),
            path: "temperature".to_string(),
            dtype: "float32".to_string(),
            shape: vec![365, 180, 360],
            chunks: vec![1, 180, 360],
            compressor: None,
            fill_value: None,
            order: "C".to_string(),
            filters: vec![],
            attributes: attrs1,
            dimensions: vec![],
        };

        let var2 = Variable {
            name: "pressure".to_string(),
            path: "pressure".to_string(),
            dtype: "float32".to_string(),
            shape: vec![365, 50], // Same time dimension, different level
            chunks: vec![1, 50],
            compressor: None,
            fill_value: None,
            order: "C".to_string(),
            filters: vec![],
            attributes: attrs2,
            dimensions: vec![],
        };

        metadata.variables.insert("temperature".to_string(), var1);
        metadata.variables.insert("pressure".to_string(), var2);
        metadata.infer_dimensions();

        // Should have 4 dimensions total
        assert_eq!(metadata.dimensions.len(), 4);

        // Time dimension should appear in both variables with same size (not unlimited)
        let time_dim = metadata.dimensions.get("time").unwrap();
        assert_eq!(time_dim.max_length, 365);
        assert!(!time_dim.is_unlimited);
        assert_eq!(time_dim.appearances.len(), 2);

        // lat, lon, and level should each appear once
        assert!(metadata.dimensions.contains_key("lat"));
        assert!(metadata.dimensions.contains_key("lon"));
        assert!(metadata.dimensions.contains_key("level"));
    }
}
