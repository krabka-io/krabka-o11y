use krabka_blockstore::{ProfileSampleRow, encode_profile_samples};

use super::{ProfileError, RecordBatch, SampleRow};

/// Encodes the in-memory rows through the block encoder, so the test store's
/// batches match the persisted `profile_samples_schema()` column for column.
pub(crate) fn encode_rows(rows: &[&SampleRow]) -> Result<RecordBatch, ProfileError> {
    let block_rows: Vec<ProfileSampleRow> = rows
        .iter()
        .map(|row| ProfileSampleRow {
            series_fingerprint: row.fingerprint,
            timestamp: row.timestamp_ms,
            profile_type: row.profile_type.clone(),
            stacktrace_id: u64::from(row.stacktrace_id),
            value: row.value,
            stacktrace_partition: row.partition,
            total_value: row.total_value,
            span_id: row.span_id,
            trace_id: row.trace_id.clone(),
            wal_sample_ids: row.wal_sample_ids.clone(),
        })
        .collect();
    encode_profile_samples(&block_rows).map_err(|err| ProfileError::Store(err.to_string()))
}
