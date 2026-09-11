use super::{
    DistributorError, LOKI_REJECT_OLD_SAMPLES_MAX_AGE, Labels, Time, TimeExt, current_unix_time_ns,
    loki_stale_sample_label_set,
};

pub(crate) fn loki_missing_proto_timestamp_error(
    stream_labels: &Labels,
    max_age: Time,
) -> DistributorError {
    // With the window off there is still an oldest acceptable timestamp to
    // report, because the entry is rejected for having no timestamp at all.
    // `Loki`'s own default is what it would report.
    let max_age = if max_age > Time::ZERO {
        max_age
    } else {
        LOKI_REJECT_OLD_SAMPLES_MAX_AGE
    };
    let oldest_acceptable_timestamp_ns = current_unix_time_ns().saturating_sub(max_age.nanos_i64());
    DistributorError::TimestampTooOldString {
        stream: loki_stale_sample_label_set(stream_labels),
        timestamp: "0001-01-01T00:00:00Z",
        oldest_acceptable_timestamp_ns,
    }
}
