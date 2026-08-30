use super::*;

pub(crate) fn log_block_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("series_fingerprint", DataType::UInt64, false),
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("line", DataType::Utf8, false),
        Field::new("structured_metadata", structured_metadata_type(), false),
    ]))
}
