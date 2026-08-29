use super::*;

#[derive(Clone)]
pub struct QuerierState {
    pub(crate) root: PathBuf,
    pub(crate) label_index: LabelIndex,
    pub(crate) block_index: BlockIndex,
    pub(crate) cold_store: Option<ColdObjectStoreState>,
    pub(crate) dynamic_index: Option<DynamicIndexSource>,
    pub(crate) dynamic_index_cache: DynamicIndexCache,
    pub(crate) cold_block_fetch_concurrency: NonZeroUsize,
    pub(crate) hot_tail: Option<HotTailState>,
    pub(crate) delete_requests: Option<SharedLogDeleteRequests>,
    pub(crate) rules: SharedLokiRules,
    pub(crate) alert_states: SharedPrometheusAlertStates,
    pub(crate) query_authorizer: Arc<dyn LogQueryAuthorizer>,
    pub(crate) max_query_range: Option<Time>,
    /// A count of series, not a data volume, so it stays a plain integer.
    pub(crate) max_query_series: Option<usize>,
    pub(crate) max_query_read: Option<ByteSize>,
    pub(crate) max_query_length: Option<ByteSize>,
    /// Shared RED-metrics bundle. It is `None` for test routers that do not
    /// wire metrics. The binary threads a shared bundle in with
    /// [`QuerierState::with_metrics`].
    pub(crate) metrics: Option<ServiceMetrics>,
}

pub(crate) type LokiRuleGroupsByName = BTreeMap<String, serde_yaml::Value>;
pub(crate) type LokiRuleNamespaces = BTreeMap<String, LokiRuleGroupsByName>;
pub(crate) type LokiRuleTenants = BTreeMap<String, LokiRuleNamespaces>;

#[derive(Clone, Default)]
pub(crate) struct SharedLokiRules {
    pub(crate) tenants: Arc<Mutex<LokiRuleTenants>>,
    pub(crate) storage_path: Option<Arc<PathBuf>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct PrometheusAlertKey {
    pub(crate) tenant: String,
    pub(crate) alert_name: String,
    pub(crate) query: String,
    pub(crate) labels: Labels,
}

#[derive(Clone, Debug)]
pub(crate) struct PrometheusAlertRuntimeState {
    pub(crate) active_at: i64,
    pub(crate) last_active_at: i64,
    pub(crate) value: String,
}

#[derive(Clone, Default)]
pub(crate) struct SharedPrometheusAlertStates {
    pub(crate) alerts: Arc<Mutex<BTreeMap<PrometheusAlertKey, PrometheusAlertRuntimeState>>>,
}

impl SharedPrometheusAlertStates {
    pub(crate) fn clear_tenant(&self, tenant: &str) {
        self.alerts
            .lock()
            .expect("Prometheus alert state lock poisoned")
            .retain(|key, _| key.tenant != tenant);
    }
}

#[derive(Clone)]
pub(crate) struct ColdObjectStoreState {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) prefix: ObjectPath,
}

#[derive(Clone)]
pub(crate) enum DynamicIndexSource {
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
pub(crate) struct DynamicIndexCache {
    pub(crate) cache_ttl: Time,
    pub(crate) shard_cache_ttl: Time,
    pub(crate) shard_fetch_concurrency: NonZeroUsize,
    pub(crate) entries: Arc<Mutex<BTreeMap<DynamicIndexCacheKey, CachedDynamicIndex>>>,
    pub(crate) shard_ranges: Arc<Mutex<BTreeMap<DynamicShardRangesCacheKey, CachedShardRanges>>>,
    pub(crate) shard_indexes: Arc<Mutex<BTreeMap<DynamicShardIndexCacheKey, CachedDynamicIndex>>>,
}

impl DynamicIndexCache {
    pub(crate) fn clear(&self) {
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

    pub(crate) fn get(&self, key: &DynamicIndexCacheKey) -> Option<(LabelIndex, BlockIndex)> {
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

    pub(crate) fn insert(
        &self,
        key: DynamicIndexCacheKey,
        label_index: LabelIndex,
        block_index: BlockIndex,
    ) {
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

    pub(crate) fn get_shard_ranges(
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

    pub(crate) fn insert_shard_ranges(
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

    pub(crate) fn get_shard_index(
        &self,
        key: &DynamicShardIndexCacheKey,
    ) -> Option<(LabelIndex, BlockIndex)> {
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

    pub(crate) fn insert_shard_index(
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
pub(crate) enum DynamicIndexCacheKey {
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
pub(crate) struct DynamicShardRangesCacheKey {
    pub(crate) tenant: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct DynamicShardIndexCacheKey {
    pub(crate) tenant: String,
    pub(crate) start_ns: i64,
    pub(crate) end_ns: i64,
}

#[derive(Clone)]
pub(crate) struct CachedDynamicIndex {
    pub(crate) loaded_at: Instant,
    pub(crate) label_index: LabelIndex,
    pub(crate) block_index: BlockIndex,
}

#[derive(Clone)]
pub(crate) struct CachedShardRanges {
    pub(crate) loaded_at: Instant,
    pub(crate) listed_from_ns: i64,
    pub(crate) ranges: Vec<TimeRange>,
}

pub(crate) fn merge_tenant_shard_indexes(
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
pub(crate) struct HotTailState {
    pub(crate) source: Arc<dyn LogHotTail>,
    pub(crate) frontier: CompactionFrontierSource,
}
