use super::Timestamp;

pub(crate) fn timestamp_micros(timestamp: Option<&Timestamp>) -> i64 {
    timestamp.map_or(0, |ts| {
        ts.seconds
            .saturating_mul(1_000_000)
            .saturating_add(i64::from(ts.nanos) / 1_000)
    })
}
