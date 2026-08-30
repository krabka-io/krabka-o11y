use super::{Arc, DataType, Field, Schema, SchemaRef, fingerprint_field, timestamp_field};

/// Metric metadata rows used by the per-tenant metadata index.
#[must_use]
pub fn metadata_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        fingerprint_field(),
        timestamp_field(),
        Field::new("metric_family_name", DataType::Utf8, false),
        Field::new("metric_type", DataType::Utf8, false),
        Field::new("help", DataType::Utf8, false),
        Field::new("unit", DataType::Utf8, false),
    ]))
}
