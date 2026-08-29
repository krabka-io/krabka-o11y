#[derive(Clone)]
pub struct QuerierState {
    root: PathBuf,
    label_index: LabelIndex,
    block_index: BlockIndex,
    cold_store: Option<ColdObjectStoreState>,
    dynamic_index: Option<DynamicIndexSource>,
    dynamic_index_cache: DynamicIndexCache,
    cold_block_fetch_concurrency: NonZeroUsize,
    hot_tail: Option<HotTailState>,
    delete_requests: Option<SharedLogDeleteRequests>,
    rules: SharedLokiRules,
    alert_states: SharedPrometheusAlertStates,
    query_authorizer: Arc<dyn LogQueryAuthorizer>,
    max_query_range: Option<Time>,
    /// A count of series, not a data volume, so it stays a plain integer.
    max_query_series: Option<usize>,
    max_query_read: Option<ByteSize>,
    max_query_length: Option<ByteSize>,
    /// Shared RED-metrics bundle. It is `None` for test routers that do not
    /// wire metrics. The binary threads a shared bundle in with
    /// [`QuerierState::with_metrics`].
    metrics: Option<ServiceMetrics>,
}

type LokiRuleGroupsByName = BTreeMap<String, serde_yaml::Value>;
type LokiRuleNamespaces = BTreeMap<String, LokiRuleGroupsByName>;
type LokiRuleTenants = BTreeMap<String, LokiRuleNamespaces>;

#[derive(Clone, Default)]
struct SharedLokiRules {
    tenants: Arc<Mutex<LokiRuleTenants>>,
    storage_path: Option<Arc<PathBuf>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct PrometheusAlertKey {
    tenant: String,
    alert_name: String,
    query: String,
    labels: Labels,
}

#[derive(Clone, Debug)]
struct PrometheusAlertRuntimeState {
    active_at: i64,
    last_active_at: i64,
    value: String,
}

#[derive(Clone, Default)]
struct SharedPrometheusAlertStates {
    alerts: Arc<Mutex<BTreeMap<PrometheusAlertKey, PrometheusAlertRuntimeState>>>,
}

impl SharedPrometheusAlertStates {
    fn clear_tenant(&self, tenant: &str) {
        self.alerts
            .lock()
            .expect("Prometheus alert state lock poisoned")
            .retain(|key, _| key.tenant != tenant);
    }
}

#[derive(Clone)]
struct ColdObjectStoreState {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
}

#[derive(Clone)]
enum DynamicIndexSource {
    TenantObjectStoreManifest {
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
    },
    TenantObjectStoreShards {
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
    },
}

#[derive(Clone)]
struct DynamicIndexCache {
    cache_ttl: Time,
    shard_cache_ttl: Time,
    shard_fetch_concurrency: NonZeroUsize,
    entries: Arc<Mutex<BTreeMap<DynamicIndexCacheKey, CachedDynamicIndex>>>,
    shard_ranges: Arc<Mutex<BTreeMap<DynamicShardRangesCacheKey, CachedShardRanges>>>,
    shard_indexes: Arc<Mutex<BTreeMap<DynamicShardIndexCacheKey, CachedDynamicIndex>>>,
}

impl DynamicIndexCache {
    fn clear(&self) {
        self.entries
            .lock()
            .expect("dynamic index cache lock poisoned")
            .clear();
        self.shard_ranges
            .lock()
            .expect("dynamic index shard range cache lock poisoned")
            .clear();
        self.shard_indexes
            .lock()
            .expect("dynamic index shard cache lock poisoned")
            .clear();
    }

    fn get(&self, key: &DynamicIndexCacheKey) -> Option<(LabelIndex, BlockIndex)> {
        let mut entries = self
            .entries
            .lock()
            .expect("dynamic index cache lock poisoned");
        let entry = entries.get(key)?;
        // `>` is a permanent mutation survivor against `>=` in all three of
        // these lookups: they differ only for an entry whose age equals its TTL
        // to the nanosecond, which a monotonic clock does not hand out.
        if entry.loaded_at.elapsed().as_time() > self.cache_ttl {
            entries.remove(key);
            return None;
        }
        Some((entry.label_index.clone(), entry.block_index.clone()))
    }

    fn insert(&self, key: DynamicIndexCacheKey, label_index: LabelIndex, block_index: BlockIndex) {
        self.entries
            .lock()
            .expect("dynamic index cache lock poisoned")
            .insert(
                key,
                CachedDynamicIndex {
                    loaded_at: Instant::now(),
                    label_index,
                    block_index,
                },
            );
    }

    fn get_shard_ranges(
        &self,
        key: &DynamicShardRangesCacheKey,
        required_from_ns: i64,
    ) -> Option<Vec<TimeRange>> {
        let mut ranges = self
            .shard_ranges
            .lock()
            .expect("dynamic index shard range cache lock poisoned");
        let entry = ranges.get(key)?;
        // The TTL comparison is a permanent survivor for the reason given at
        // `DynamicIndexCache::get`.
        if entry.loaded_at.elapsed().as_time() > self.cache_ttl
            || entry.listed_from_ns > required_from_ns
        {
            ranges.remove(key);
            return None;
        }
        Some(entry.ranges.clone())
    }

    fn insert_shard_ranges(
        &self,
        key: DynamicShardRangesCacheKey,
        listed_from_ns: i64,
        ranges: Vec<TimeRange>,
    ) {
        self.shard_ranges
            .lock()
            .expect("dynamic index shard range cache lock poisoned")
            .insert(
                key,
                CachedShardRanges {
                    loaded_at: Instant::now(),
                    listed_from_ns,
                    ranges,
                },
            );
    }

    fn get_shard_index(&self, key: &DynamicShardIndexCacheKey) -> Option<(LabelIndex, BlockIndex)> {
        let mut entries = self
            .shard_indexes
            .lock()
            .expect("dynamic index shard cache lock poisoned");
        let entry = entries.get(key)?;
        // The TTL comparison is a permanent survivor for the reason given at
        // `DynamicIndexCache::get`.
        if entry.loaded_at.elapsed().as_time() > self.shard_cache_ttl {
            entries.remove(key);
            return None;
        }
        Some((entry.label_index.clone(), entry.block_index.clone()))
    }

    fn insert_shard_index(
        &self,
        key: DynamicShardIndexCacheKey,
        label_index: LabelIndex,
        block_index: BlockIndex,
    ) {
        self.shard_indexes
            .lock()
            .expect("dynamic index shard cache lock poisoned")
            .insert(
                key,
                CachedDynamicIndex {
                    loaded_at: Instant::now(),
                    label_index,
                    block_index,
                },
            );
    }
}

impl Default for DynamicIndexCache {
    fn default() -> Self {
        Self {
            cache_ttl: secs(5),
            shard_cache_ttl: minutes(5),
            shard_fetch_concurrency: NonZeroUsize::new(32)
                .expect("default shard fetch concurrency is nonzero"),
            entries: Arc::default(),
            shard_ranges: Arc::default(),
            shard_indexes: Arc::default(),
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum DynamicIndexCacheKey {
    TenantManifest {
        tenant: String,
    },
    TenantShards {
        tenant: String,
        start_ns: i64,
        end_ns: i64,
    },
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DynamicShardRangesCacheKey {
    tenant: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DynamicShardIndexCacheKey {
    tenant: String,
    start_ns: i64,
    end_ns: i64,
}

#[derive(Clone)]
struct CachedDynamicIndex {
    loaded_at: Instant,
    label_index: LabelIndex,
    block_index: BlockIndex,
}

#[derive(Clone)]
struct CachedShardRanges {
    loaded_at: Instant,
    listed_from_ns: i64,
    ranges: Vec<TimeRange>,
}

fn merge_tenant_shard_indexes(
    tenant: &str,
    indexes: impl IntoIterator<Item = (LabelIndex, BlockIndex)>,
) -> (LabelIndex, BlockIndex) {
    let mut merged_labels = LabelIndex::default();
    let mut merged_blocks = BTreeMap::new();

    for (label_index, block_index) in indexes {
        for (_, labels) in label_index.tenant_series(tenant) {
            merged_labels.insert_series(tenant.to_string(), labels);
        }
        for block in block_index.blocks() {
            merged_blocks
                .entry(block.key.object_key())
                .or_insert_with(|| block.clone());
        }
    }

    let mut block_index = BlockIndex::default();
    for block in merged_blocks.into_values() {
        block_index.insert(block);
    }

    (merged_labels, block_index)
}

#[derive(Clone)]
struct HotTailState {
    source: Arc<dyn LogHotTail>,
    frontier: CompactionFrontierSource,
}

