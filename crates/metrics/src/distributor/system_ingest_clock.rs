use super::{IngestClock, Instant, SystemTime, UNIX_EPOCH};

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
