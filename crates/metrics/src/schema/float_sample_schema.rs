use super::{Arc, DataType, Field, Schema, SchemaRef, fingerprint_field, timestamp_field};

/// Float samples, which are counters, gauges, and classic histogram bucket
/// series.
#[must_use]
pub fn float_sample_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        fingerprint_field(),
        timestamp_field(),
        Field::new("value", DataType::Float64, false),
    ]))
}
