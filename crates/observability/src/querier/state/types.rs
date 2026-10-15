use krabka_units::convert::StdDurationExt;

use crate::{
    Arc, BTreeMap, BlockIndex, CompactionFrontierSource, ExecutionOptions, InMemoryCache, Instant,
    LabelIndex, Labels, Limits, LogHotTail, LogQueryAuthorizer, Mutex, NonZeroUsize, ObjectPath,
    ObjectStore, OverridesProvider, PathBuf, ServiceMetrics, SharedLogDeleteRequests, Time,
    TimeRange, Value, minutes, secs,
};

mod dynamic_index_cache;
mod merge_tenant_shard_indexes;
mod querier_state;

pub(crate) use dynamic_index_cache::DynamicIndexCache;
pub(crate) use merge_tenant_shard_indexes::merge_tenant_shard_indexes;
pub use querier_state::QuerierState;

#[derive(Clone)]
pub(crate) struct TenantIndexPair {
    pub(crate) label_index: Arc<LabelIndex>,
    pub(crate) block_index: Arc<BlockIndex>,
}

#[derive(Clone)]
pub(crate) struct CachedDynamicIndex {
    pub(crate) loaded_at: Instant,
    pub(crate) label_index: Arc<LabelIndex>,
    pub(crate) block_index: Arc<BlockIndex>,
}

#[derive(Clone)]
pub(crate) struct CachedShardRanges {
    pub(crate) loaded_at: Instant,
    pub(crate) listed_from_ns: i64,
    pub(crate) ranges: Vec<TimeRange>,
}

#[derive(Clone)]
pub(crate) struct ColdObjectStoreState {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) prefix: ObjectPath,
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

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct DynamicShardIndexCacheKey {
    pub(crate) tenant: String,
    pub(crate) start_ns: i64,
    pub(crate) end_ns: i64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct DynamicShardRangesCacheKey {
    pub(crate) tenant: String,
}

#[derive(Clone)]
pub(crate) struct HotTailState {
    pub(crate) source: Arc<dyn LogHotTail>,
    pub(crate) frontier: CompactionFrontierSource,
}

pub(crate) type LokiRuleGroupsByName = BTreeMap<String, serde_yaml::Value>;

pub(crate) type LokiRuleNamespaces = BTreeMap<String, LokiRuleGroupsByName>;

pub(crate) type LokiRuleTenants = BTreeMap<String, LokiRuleNamespaces>;

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
pub(crate) struct SharedLokiRules {
    pub(crate) tenants: Arc<Mutex<LokiRuleTenants>>,
    pub(crate) storage_path: Option<Arc<PathBuf>>,
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
