use super::*;

pub struct DistributorState {
    pub sink: Arc<dyn WalSink>,
    pub limits: TenantLimitConfig,
    pub profile_overrides: OverridesProvider,
    pub active_series: Mutex<HashMap<String, BTreeSet<u64>>>,
    pub ingestion_buckets: Mutex<HashMap<String, Arc<TokenBucket>>>,
    pub relabel: Vec<RelabelConfig>,
    /// Shared raw, Connect, and decompressed request-body limit.
    pub max_decompressed: ByteSize,
    /// Maximum tenants retained in distributor accounting maps.
    pub max_tracked_tenants: usize,
    pub legacy_decode_limits: LegacyDecodeLimits,
    /// Prometheus metrics bundle. Each ingest handler boundary calls
    /// `record_ingest`. The WAL-append error site inside [`process_raw`] calls
    /// `record_wal_append_failure`.
    pub metrics: ServiceMetrics,
}
