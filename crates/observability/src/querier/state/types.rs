use krabka_units::convert::StdDurationExt;

use crate::{
    Arc, BTreeMap, BlockIndex, ByteSize, CompactionFrontierSource, Instant, LabelIndex, Labels,
    LogHotTail, LogQueryAuthorizer, Mutex, NonZeroUsize, ObjectPath, ObjectStore, PathBuf,
    ServiceMetrics, SharedLogDeleteRequests, Time, TimeRange, minutes, secs,
};

// === split-modules: generated submodules ===
mod cached_dynamic_index;
mod cached_shard_ranges;
mod cold_object_store_state;
mod dynamic_index_cache;
mod dynamic_index_cache_key;
mod dynamic_index_source;
mod dynamic_shard_index_cache_key;
mod dynamic_shard_ranges_cache_key;
mod hot_tail_state;
mod loki_rule_groups_by_name;
mod loki_rule_namespaces;
mod loki_rule_tenants;
mod merge_tenant_shard_indexes;
mod prometheus_alert_key;
mod prometheus_alert_runtime_state;
mod querier_state;
mod shared_loki_rules;
mod shared_prometheus_alert_states;

pub (crate) use cached_dynamic_index::CachedDynamicIndex;
pub (crate) use cached_shard_ranges::CachedShardRanges;
pub (crate) use cold_object_store_state::ColdObjectStoreState;
pub (crate) use dynamic_index_cache::DynamicIndexCache;
pub (crate) use dynamic_index_cache_key::DynamicIndexCacheKey;
pub (crate) use dynamic_index_source::DynamicIndexSource;
pub (crate) use dynamic_shard_index_cache_key::DynamicShardIndexCacheKey;
pub (crate) use dynamic_shard_ranges_cache_key::DynamicShardRangesCacheKey;
pub (crate) use hot_tail_state::HotTailState;
pub (crate) use loki_rule_groups_by_name::LokiRuleGroupsByName;
pub (crate) use loki_rule_namespaces::LokiRuleNamespaces;
pub (crate) use loki_rule_tenants::LokiRuleTenants;
pub (crate) use merge_tenant_shard_indexes::merge_tenant_shard_indexes;
pub (crate) use prometheus_alert_key::PrometheusAlertKey;
pub (crate) use prometheus_alert_runtime_state::PrometheusAlertRuntimeState;
pub use querier_state::QuerierState;
pub (crate) use shared_loki_rules::SharedLokiRules;
pub (crate) use shared_prometheus_alert_states::SharedPrometheusAlertStates;
