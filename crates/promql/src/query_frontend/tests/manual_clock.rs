use super::*;

/// Deterministic clock whose epoch-millis reading the test can advance. The
/// test can then exercise TTL expiry without a sleep.
#[derive(Default)]
pub(crate) struct ManualClock {
    pub(crate) now_ms: std::sync::atomic::AtomicI64,
}

impl ManualClock {
    pub(crate) fn new(now_ms: i64) -> Self {
        Self {
            now_ms: std::sync::atomic::AtomicI64::new(now_ms),
        }
    }

    pub(crate) fn advance(&self, delta_ms: i64) {
        self.now_ms
            .fetch_add(delta_ms, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_epoch_millis(&self) -> i64 {
        self.now_ms.load(std::sync::atomic::Ordering::SeqCst)
    }
}
