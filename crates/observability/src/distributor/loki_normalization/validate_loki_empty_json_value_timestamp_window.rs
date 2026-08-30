use super::*;

pub(crate) fn validate_loki_empty_json_value_timestamp_window(
    stream_labels: &Labels,
    max_age: Option<Time>,
) -> Result<(), DistributorError> {
    let Some(max_age) = max_age else {
        return Ok(());
    };
    let oldest_acceptable_timestamp_ns = current_unix_time_ns().saturating_sub(max_age.nanos_i64());
    Err(DistributorError::TimestampTooOldString {
        stream: loki_stale_sample_label_set(stream_labels),
        timestamp: "0001-01-01T00:00:00Z",
        oldest_acceptable_timestamp_ns,
    })
}
