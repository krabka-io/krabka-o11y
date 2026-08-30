use super::*;

/// This ingester's own clock, at the moment a clock batch arrives.
///
/// A clock before the epoch, or one past the `i64` nanosecond ceiling in the
/// year 2262, saturates rather than wrapping. Either reading is already a
/// broken host clock, and the skew series is what says so.
pub(crate) fn ingest_stamp() -> UnixNanos {
    UnixNanos::new(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX)
            }),
    )
}
