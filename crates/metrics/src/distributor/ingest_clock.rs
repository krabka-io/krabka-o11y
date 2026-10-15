use super::*;

/// Monotonic clock that stamps series and stream activity on the ingest path.
///
/// This is an abstraction, so a test can drive an idle timeout deterministically
/// and does not need a real wall-clock wait.
pub trait IngestClock: Send + Sync + std::fmt::Debug {
    fn now(&self) -> Instant;

    /// Current wall-clock time in Unix epoch milliseconds.
    fn now_unix_ms(&self) -> i64;
}

/// Real monotonic clock backed by [`std::time::Instant::now`].
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemIngestClock;

impl IngestClock for SystemIngestClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn now_unix_ms(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| {
                i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
            })
    }
}

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
