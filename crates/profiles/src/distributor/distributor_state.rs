use super::{
    Arc, BTreeSet, ByteSize, HashMap, LegacyDecodeLimits, Mutex, OverridesProvider, RelabelConfig,
    ServiceMetrics, TenantPolicy, TokenBucket, WalSink,
};

pub(crate) type CumulativeProfileCache =
    HashMap<(String, Vec<(String, String)>), HashMap<Vec<u32>, i64>>;

pub struct DistributorState {
    pub sink: Arc<dyn WalSink>,
    /// The one place a per-tenant limit is resolved. Every ingest gate reads it
    /// through [`OverridesProvider::for_tenant`], so a tenant with no entry of
    /// its own gets the file's defaults rather than a second set of caps.
    pub overrides: OverridesProvider,
    /// What a push that names no tenant resolves to.
    ///
    /// Every ingest door resolves the `X-Scope-OrgID` header under this one
    /// value. The binary sets [`TenantPolicy::anonymous`], because Pyroscope
    /// keeps multi-tenancy off by default and puts such a push under the
    /// `anonymous` tenant.
    pub tenant_policy: TenantPolicy,
    pub active_series: Mutex<HashMap<String, BTreeSet<u64>>>,
    pub cumulative_profiles: tokio::sync::Mutex<CumulativeProfileCache>,
    pub ingestion_buckets: Mutex<HashMap<String, Arc<TokenBucket>>>,
    pub relabel: Vec<RelabelConfig>,
    /// Shared raw, Connect, and decompressed request-body limit.
    pub max_decompressed: ByteSize,
    /// Maximum tenants retained in distributor accounting maps.
    pub max_tracked_tenants: usize,
    pub legacy_decode_limits: LegacyDecodeLimits,
    /// Prometheus metrics bundle. Each ingest handler boundary calls
    /// `record_ingest`. The WAL-append error site inside
    /// [`super::process_raw`] calls `record_wal_append_failure`.
    pub metrics: ServiceMetrics,
}
