use super::{IngestClock, Instant};

/// Real monotonic clock backed by [`std::time::Instant::now`].
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemIngestClock;

impl IngestClock for SystemIngestClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}
