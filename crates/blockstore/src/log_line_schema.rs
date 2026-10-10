use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

/// The schema of a minimal log block: the two mandatory columns and a
/// nullable `line` payload.
pub(crate) fn log_line_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
        Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
        Field::new("line", DataType::Utf8, true),
    ]))
}
