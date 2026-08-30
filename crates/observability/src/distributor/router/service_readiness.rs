use super::*;

#[derive(Clone)]
pub(crate) struct ServiceReadiness {
    pub(crate) wal_connected: Arc<AtomicBool>,
    pub(crate) authorization_connected: Arc<AtomicBool>,
}

impl ServiceReadiness {
    pub(crate) fn ready() -> Self {
        Self {
            wal_connected: Arc::new(AtomicBool::new(true)),
            authorization_connected: Arc::new(AtomicBool::new(true)),
        }
    }

    pub(crate) fn deferred_querier() -> Self {
        Self {
            wal_connected: Arc::new(AtomicBool::new(false)),
            authorization_connected: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.wal_connected.load(AtomicOrdering::SeqCst)
            && self.authorization_connected.load(AtomicOrdering::SeqCst)
    }
}
