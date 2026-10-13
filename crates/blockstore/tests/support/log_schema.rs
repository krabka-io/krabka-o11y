use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

pub(crate) fn schema() -> SchemaRef {
    std::sync::Arc::new(Schema::new(vec![
        Field::new(krabka_blockstore::COL_FINGERPRINT, DataType::UInt64, false),
        Field::new(krabka_blockstore::COL_TIMESTAMP, DataType::Int64, false),
        Field::new("line", DataType::Utf8, true),
    ]))
}
