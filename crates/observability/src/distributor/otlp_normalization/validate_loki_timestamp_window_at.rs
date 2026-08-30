use super::{DistributorError, Labels, Time, TimeExt, loki_stale_sample_label_set};

/// The window check against a caller-supplied `now`.
///
/// Split out so the two bounds can be tested exactly at their edges. Both are
/// strict comparisons -- a timestamp precisely at the oldest or newest
/// acceptable value is accepted -- and against a wall clock that boundary is
/// unreachable: `now` advances between choosing the timestamp and reading it.
pub(crate) fn validate_loki_timestamp_window_at(
    timestamp_ns: i64,
    now_ns: i64,
    stream_labels: &Labels,
    max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<(), DistributorError> {
    if let Some(max_age) = max_age {
        let oldest_acceptable_timestamp_ns = now_ns.saturating_sub(max_age.nanos_i64());
        if timestamp_ns < oldest_acceptable_timestamp_ns {
            return Err(DistributorError::TimestampTooOld {
                stream: loki_stale_sample_label_set(stream_labels),
                timestamp_ns,
                oldest_acceptable_timestamp_ns,
            });
        }
    }
    if let Some(creation_grace_period) = creation_grace_period {
        let newest_acceptable_timestamp_ns =
            now_ns.saturating_add(creation_grace_period.nanos_i64());
        if timestamp_ns > newest_acceptable_timestamp_ns {
            return Err(DistributorError::TimestampTooNew {
                stream: loki_stale_sample_label_set(stream_labels),
                timestamp_ns,
            });
        }
    }
    Ok(())
}
