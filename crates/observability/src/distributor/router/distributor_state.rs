use super::*;

#[derive(Clone)]
pub struct DistributorState {
    pub(crate) sink: Arc<dyn LogWalSink>,
    pub(crate) ingest_limiter: Arc<dyn LogIngestLimiter>,
    pub(crate) prepare_shutdown: Arc<AtomicBool>,
    pub(crate) max_ingest_body: Option<ByteSize>,
    pub(crate) wal_append_timeout: Option<Time>,
    pub(crate) reject_old_samples_max_age: Option<Time>,
    pub(crate) creation_grace_period: Option<Time>,
    pub(crate) metrics: ServiceMetrics,
}
