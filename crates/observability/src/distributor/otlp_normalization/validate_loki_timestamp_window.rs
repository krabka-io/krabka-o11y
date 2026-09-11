use super::{
    DistributorError, Labels, Limits, current_unix_time_ns, validate_loki_timestamp_window_at,
};

pub(crate) fn validate_loki_timestamp_window(
    timestamp_ns: i64,
    stream_labels: &Labels,
    limits: &Limits,
) -> Result<(), DistributorError> {
    validate_loki_timestamp_window_at(
        timestamp_ns,
        current_unix_time_ns(),
        stream_labels,
        limits.reject_old_samples_max_age,
        limits.creation_grace_period,
    )
}
