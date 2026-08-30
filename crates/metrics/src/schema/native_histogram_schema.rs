use super::{Arc, COL_NH_COUNT, COL_NH_CUSTOM_VALUES, COL_NH_IS_FLOAT, COL_NH_NEG_COUNTS, COL_NH_NEG_SPANS, COL_NH_POS_COUNTS, COL_NH_POS_SPANS, COL_NH_RESET_HINT, COL_NH_SCHEMA, COL_NH_START_TS, COL_NH_SUM, COL_NH_ZERO_COUNT, COL_NH_ZERO_THRESHOLD, DataType, Field, Schema, SchemaRef, f64_list_type, fingerprint_field, span_list_type, timestamp_field};

/// Native histogram samples with absolute bucket counts.
#[must_use]
pub fn native_histogram_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        fingerprint_field(),
        timestamp_field(),
        Field::new(COL_NH_SCHEMA, DataType::Int8, false),
        Field::new(COL_NH_IS_FLOAT, DataType::Boolean, false),
        Field::new(COL_NH_RESET_HINT, DataType::Int8, false),
        Field::new(COL_NH_ZERO_THRESHOLD, DataType::Float64, false),
        Field::new(COL_NH_ZERO_COUNT, DataType::Float64, false),
        Field::new(COL_NH_COUNT, DataType::Float64, false),
        Field::new(COL_NH_SUM, DataType::Float64, false),
        Field::new(COL_NH_POS_SPANS, span_list_type(), false),
        Field::new(COL_NH_POS_COUNTS, f64_list_type(), false),
        Field::new(COL_NH_NEG_SPANS, span_list_type(), false),
        Field::new(COL_NH_NEG_COUNTS, f64_list_type(), false),
        Field::new(COL_NH_CUSTOM_VALUES, f64_list_type(), true),
        Field::new(COL_NH_START_TS, DataType::Int64, true),
    ]))
}
