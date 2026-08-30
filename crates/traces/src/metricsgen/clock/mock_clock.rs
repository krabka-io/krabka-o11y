use super::{Arc, AtomicI64, Clock, Ordering};

/// Deterministic test clock.
#[derive(Debug, Clone)]
pub struct MockClock {
    pub(crate) now: Arc<AtomicI64>,
}

impl MockClock {
    #[must_use]
    pub fn new(start_ns: i64) -> Self {
        Self {
            now: Arc::new(AtomicI64::new(start_ns)),
        }
    }

    pub fn advance(&self, ns: i64) {
        self.now.fetch_add(ns, Ordering::SeqCst);
    }

    pub fn set(&self, ns: i64) {
        self.now.store(ns, Ordering::SeqCst);
    }
}

impl Clock for MockClock {
    fn now_ns(&self) -> i64 {
        self.now.load(Ordering::SeqCst)
    }
}
