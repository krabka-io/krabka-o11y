use super::{
    BinaryBuilder, BlockStoreError, Int32Type, Int64Builder, ProfileSampleRow, RecordBatch, Result,
    StringDictionaryBuilder, UInt64Builder, profile_samples_schema,
};

/// Encodes rows into a `RecordBatch` that matches `profile_samples_schema()`.
///
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn encode_profile_samples(rows: &[ProfileSampleRow]) -> Result<RecordBatch> {
    let mut columns = ProfileSampleColumns::new();

    for row in rows {
        columns.fp.append_value(row.series_fingerprint);
        columns.ts.append_value(row.timestamp);
        columns
            .profile_type
            .append(&row.profile_type)
            .map_err(|err| BlockStoreError::InvalidBlock(err.to_string()))?;
        columns.stacktrace_id.append_value(row.stacktrace_id);
        columns.value.append_value(row.value);
        columns.partition.append_value(row.stacktrace_partition);
        columns.total_value.append_value(row.total_value);
        match row.span_id {
            Some(value) => columns.span_id.append_value(value),
            None => columns.span_id.append_null(),
        }
        for id in &row.wal_sample_ids {
            columns.wal_sample_ids.values().append_value(id);
        }
        columns.wal_sample_ids.append(true);
        match &row.trace_id {
            Some(value) => columns.trace_id.append_value(value),
            None => columns.trace_id.append_null(),
        }
    }

    RecordBatch::try_new(profile_samples_schema(), columns.finish())
        .map_err(|err| BlockStoreError::InvalidBlock(err.to_string()))
}

#[derive(krabka_column_macros::ColumnBuilders)]
struct ProfileSampleColumns {
    fp: UInt64Builder,
    ts: Int64Builder,
    profile_type: StringDictionaryBuilder<Int32Type>,
    stacktrace_id: UInt64Builder,
    value: Int64Builder,
    partition: UInt64Builder,
    total_value: Int64Builder,
    span_id: UInt64Builder,
    trace_id: BinaryBuilder,
    #[column(init = "arrow::array::ListBuilder::new(BinaryBuilder::new())")]
    wal_sample_ids: arrow::array::ListBuilder<BinaryBuilder>,
}
