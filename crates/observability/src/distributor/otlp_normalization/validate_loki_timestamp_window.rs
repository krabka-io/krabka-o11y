use super::*;

pub(crate) fn validate_loki_timestamp_window(
    timestamp_ns: i64,
    stream_labels: &Labels,
    max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<(), DistributorError> {
    validate_loki_timestamp_window_at(
        timestamp_ns,
        current_unix_time_ns(),
        stream_labels,
        max_age,
        creation_grace_period,
    )
}
