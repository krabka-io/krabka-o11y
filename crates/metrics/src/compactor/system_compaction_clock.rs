use super::*;

/// Real monotonic clock backed by [`std::time::Instant::now`].
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemCompactionClock;

impl CompactionClock for SystemCompactionClock {
    fn now(&self) -> std::time::Instant {
        std::time::Instant::now()
    }
}
