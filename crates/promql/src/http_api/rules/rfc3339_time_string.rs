use super::*;

pub(crate) fn rfc3339_time_string(ts_ms: i64) -> String {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(ts_ms) * 1_000_000).map_or_else(
        |_| zero_evaluation_time().to_string(),
        |time| {
            time.format(&Rfc3339)
                .unwrap_or_else(|_| zero_evaluation_time().to_string())
        },
    )
}
