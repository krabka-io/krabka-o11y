use super::{BuiltSample, ProfileSampleRow, ProfilesError, RecordBatch, encode_profile_samples};

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn samples_batch(rows: &[BuiltSample]) -> Result<RecordBatch, ProfilesError> {
    let rows = rows
        .iter()
        .map(|row| ProfileSampleRow {
            series_fingerprint: row.series_fingerprint,
            timestamp: row.timestamp_ns,
            profile_type: row.profile_type.clone(),
            stacktrace_id: row.stacktrace_id,
            value: row.value,
            stacktrace_partition: row.stacktrace_partition,
            total_value: row.total_value,
            span_id: row.span_id,
            trace_id: row.trace_id.clone(),
        })
        .collect::<Vec<_>>();
    encode_profile_samples(&rows).map_err(|err| ProfilesError::Block(err.to_string()))
}
