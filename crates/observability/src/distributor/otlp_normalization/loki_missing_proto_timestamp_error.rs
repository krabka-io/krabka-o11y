use super::*;

pub(crate) fn loki_missing_proto_timestamp_error(
    stream_labels: &Labels,
    max_age: Option<Time>,
) -> DistributorError {
    let max_age = max_age.unwrap_or(LOKI_REJECT_OLD_SAMPLES_MAX_AGE);
    let oldest_acceptable_timestamp_ns = current_unix_time_ns().saturating_sub(max_age.nanos_i64());
    DistributorError::TimestampTooOldString {
        stream: loki_stale_sample_label_set(stream_labels),
        timestamp: "0001-01-01T00:00:00Z",
        oldest_acceptable_timestamp_ns,
    }
}
