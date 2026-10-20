use super::{
    Arc, BTreeSet, ByteSize, HashMap, LegacyDecodeLimits, Mutex, OverridesProvider, RelabelConfig,
    ServiceMetrics, TenantPolicy, TokenBucket, WalSink,
};

#[doc(hidden)]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CumulativeProfileKey {
    pub(crate) tenant: String,
    pub(crate) labels: Vec<krabka_blockstore::LabelPair>,
}

#[doc(hidden)]
pub type CumulativeProfileCache = HashMap<CumulativeProfileKey, HashMap<Vec<u32>, i64>>;

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
    /// Serializes cumulative transactions and max-series reservations per tenant.
    /// Weak entries keep idle tenant locks reclaimable without breaking waiters.
    pub tenant_ingest_locks: Mutex<HashMap<String, std::sync::Weak<tokio::sync::Mutex<()>>>>,
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

impl DistributorState {
    pub(crate) fn tenant_ingest_lock(
        &self,
        tenant: &str,
    ) -> Result<Arc<tokio::sync::Mutex<()>>, super::ProfilesError> {
        let mut locks = self.tenant_ingest_locks.lock().map_err(|_| {
            super::ProfilesError::Internal("tenant ingest lock poisoned".to_owned())
        })?;
        if let Some(lock) = locks.get(tenant).and_then(std::sync::Weak::upgrade) {
            return Ok(lock);
        }
        // Reap idle entries at the accounting cap. Live locks must never be
        // evicted: another request may still hold or be waiting on them.
        if locks.len() >= self.max_tracked_tenants {
            locks.retain(|_, lock| lock.strong_count() != 0);
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(tenant.to_owned(), Arc::downgrade(&lock));
        Ok(lock)
    }
}
