use super::{
    Arc, Limits, LogIngestLimiter, LogWalSink, OverridesProvider, ReadinessGate, ServiceMetrics,
    TenantId, Time,
};

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
    /// The one provider this service resolves every tenant's limits through.
    ///
    /// It is shared with the querier, so an ingest gate and a read gate answer
    /// the same tenant with the same numbers.
    pub(crate) overrides: Arc<OverridesProvider>,
    pub(crate) wal_append_timeout: Option<Time>,
    pub(crate) metrics: ServiceMetrics,
}

impl DistributorState {
    /// The limits that apply to one push.
    ///
    /// The ingest byte rate is not among them. The broker owns that quota, and
    /// `BrokerBackedIngestLimiter` enforces it. See [`crate::Limits`].
    pub(crate) fn limits_for(&self, tenant: &TenantId) -> &Limits {
        self.overrides.for_tenant(tenant)
    }
}
