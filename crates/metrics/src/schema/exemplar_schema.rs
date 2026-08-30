use super::{
    Arc, DataType, Field, Schema, SchemaRef, fingerprint_field, timestamp_field, utf8_map_field,
};

/// Exemplars whose trace and span identifiers are first-class columns.
#[must_use]
pub fn exemplar_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        fingerprint_field(),
        timestamp_field(),
        Field::new("value", DataType::Float64, false),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        utf8_map_field("labels", false),
    ]))
}
