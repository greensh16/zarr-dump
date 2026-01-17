pub mod metadata;
pub mod plot;
pub mod store;

// Re-export commonly used types for tests
pub use metadata::{AttributeValue, Dimension, DimensionInfo, Group, Variable, ZarrMetadata};
pub use plot::PlotSelection;
pub use store::ZarrStore;
