use super::{Arc, ByteSize, LogIngestLimiter, LogWalSink, ReadinessGate, ServiceMetrics, Time};

#[derive(Clone)]
pub struct DistributorState {
    pub(crate) sink: Arc<dyn LogWalSink>,
    pub(crate) ingest_limiter: Arc<dyn LogIngestLimiter>,
    /// The readiness gate an operator's drain request drops.
    ///
    /// See [`DRAINING_GATE`](crate::DRAINING_GATE): marking it unmet is what
    /// `/ingester/prepare_shutdown` does instead of leaving a ring, and it is
    /// what makes `/ready` answer 503 while a pod drains.
    pub(crate) prepare_shutdown: ReadinessGate,
    pub(crate) max_ingest_body: Option<ByteSize>,
    pub(crate) wal_append_timeout: Option<Time>,
    pub(crate) reject_old_samples_max_age: Option<Time>,
    pub(crate) creation_grace_period: Option<Time>,
    pub(crate) metrics: ServiceMetrics,
}
