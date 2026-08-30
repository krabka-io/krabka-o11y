use super::{Arc, WalSink, HaElectionSink, HaTracker, Mutex, DeltaAccumulator, IngestEnforcer, OverridesProvider, BTreeMap, BTreeSet, SeriesFingerprint, TenantLimits, Time, ByteSize, ServiceMetrics, DEFAULT_HA_FAILOVER_TIMEOUT, DEFAULT_DISTRIBUTOR_MAX_DECOMPRESSED, Limits, tenant_limits_to_limits};

/// Shared distributor handler state.
pub struct DistributorState {
    pub(crate) sink: Arc<dyn WalSink>,
    pub(crate) ha_election_sink: Option<Arc<dyn HaElectionSink>>,
    pub(crate) tracker: HaTracker,
    pub(crate) otlp_delta_accumulator: Mutex<DeltaAccumulator>,
    pub(crate) ingest_enforcer: IngestEnforcer,
    pub(crate) overrides: Option<OverridesProvider>,
    pub(crate) active_series: Mutex<BTreeMap<String, BTreeSet<SeriesFingerprint>>>,
    pub(crate) latest_timestamps: Mutex<BTreeMap<(String, SeriesFingerprint), i64>>,
    pub(crate) limits: TenantLimits,
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
            otlp_delta_accumulator: Mutex::new(DeltaAccumulator::default()),
            ingest_enforcer: IngestEnforcer::new(),
            overrides: None,
            active_series: Mutex::new(BTreeMap::new()),
            latest_timestamps: Mutex::new(BTreeMap::new()),
            limits: TenantLimits::default(),
            ha_failover_timeout: DEFAULT_HA_FAILOVER_TIMEOUT,
            max_decompressed: DEFAULT_DISTRIBUTOR_MAX_DECOMPRESSED,
            metrics: None,
        }
    }

    #[must_use]
    pub fn with_limits(mut self, limits: TenantLimits) -> Self {
        self.limits = limits;
        self
    }

    #[must_use]
    pub fn with_metrics(mut self, metrics: ServiceMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    #[must_use]
    pub fn with_overrides(mut self, overrides: OverridesProvider) -> Self {
        self.overrides = Some(overrides);
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
    pub(crate) fn limits_for_tenant(&self, tenant: &str) -> Limits {
        self.overrides.as_ref().map_or_else(
            || tenant_limits_to_limits(&self.limits),
            |overrides| overrides.for_tenant(tenant).clone(),
        )
    }
}
