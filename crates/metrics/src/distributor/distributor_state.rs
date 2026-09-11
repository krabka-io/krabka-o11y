use super::{
    Arc, ByteSize, DEFAULT_DISTRIBUTOR_MAX_DECOMPRESSED, DEFAULT_HA_FAILOVER_TIMEOUT,
    DEFAULT_MAX_TRACKED_TENANTS, HaElectionSink, HaTracker, IngestClock, IngestEnforcer, Limits,
    OverridesProvider, SeriesTracker, ServiceMetrics, SystemIngestClock, TenantDeltaAccumulators,
    TenantId, Time, WalSink,
};

/// Shared distributor handler state.
pub struct DistributorState {
    pub(crate) sink: Arc<dyn WalSink>,
    pub(crate) ha_election_sink: Option<Arc<dyn HaElectionSink>>,
    pub(crate) tracker: HaTracker,
    pub(crate) otlp_delta_accumulators: TenantDeltaAccumulators,
    pub(crate) ingest_enforcer: IngestEnforcer,
    /// The one place limits come from. Every gate reads the tenant's limits
    /// through it, so a tenant has a single limit set rather than one per gate.
    pub(crate) overrides: OverridesProvider,
    pub(crate) series_tracker: SeriesTracker,
    pub(crate) clock: Arc<dyn IngestClock>,
    pub(crate) ha_failover_timeout: Time,
    pub(crate) max_decompressed: ByteSize,
    pub(crate) metrics: Option<ServiceMetrics>,
}

impl DistributorState {
    #[must_use]
    pub fn new(sink: Arc<dyn WalSink>) -> Self {
        Self {
            sink,
            ha_election_sink: None,
            tracker: HaTracker::default(),
            otlp_delta_accumulators: TenantDeltaAccumulators::new(DEFAULT_MAX_TRACKED_TENANTS),
            ingest_enforcer: IngestEnforcer::new(),
            overrides: OverridesProvider::new(Limits::default()),
            series_tracker: SeriesTracker::new(DEFAULT_MAX_TRACKED_TENANTS),
            clock: Arc::new(SystemIngestClock),
            ha_failover_timeout: DEFAULT_HA_FAILOVER_TIMEOUT,
            max_decompressed: DEFAULT_DISTRIBUTOR_MAX_DECOMPRESSED,
            metrics: None,
        }
    }

    /// Gives every tenant the same limits. This is
    /// [`with_overrides`](Self::with_overrides) with no per-tenant entries.
    #[must_use]
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.overrides = OverridesProvider::new(limits);
        self
    }

    #[must_use]
    pub fn with_metrics(mut self, metrics: ServiceMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    #[must_use]
    pub fn with_overrides(mut self, overrides: OverridesProvider) -> Self {
        self.overrides = overrides;
        self
    }

    #[must_use]
    pub fn with_max_decompressed(mut self, max_decompressed: ByteSize) -> Self {
        self.max_decompressed = max_decompressed;
        self
    }

    #[must_use]
    pub fn with_ha_failover_timeout(mut self, timeout: Time) -> Self {
        self.ha_failover_timeout = timeout;
        self
    }

    #[must_use]
    pub fn with_max_rate_buckets(mut self, cap: usize) -> Self {
        self.ingest_enforcer = IngestEnforcer::with_max_rate_buckets(cap);
        self
    }

    /// Bounds the number of tenants the series tracker and the OTLP delta
    /// accumulators hold state for. A cap of `0` clamps to `1`.
    #[must_use]
    pub fn with_max_tracked_tenants(mut self, cap: usize) -> Self {
        self.series_tracker = SeriesTracker::new(cap);
        self.otlp_delta_accumulators = TenantDeltaAccumulators::new(cap);
        self
    }

    /// Drives the idle timeouts from `clock` instead of the system clock.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn IngestClock>) -> Self {
        self.clock = clock;
        self
    }

    #[must_use]
    pub fn with_ha_election_sink(mut self, sink: Arc<dyn HaElectionSink>) -> Self {
        self.ha_election_sink = Some(sink);
        self
    }

    #[must_use]
    pub fn tracker(&self) -> &HaTracker {
        &self.tracker
    }
}

impl DistributorState {
    pub(crate) fn limits_for_tenant(&self, tenant: &TenantId) -> &Limits {
        self.overrides.for_tenant(tenant.as_str())
    }
}
