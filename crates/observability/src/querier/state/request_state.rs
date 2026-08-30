use krabka_units::convert::StdDurationExt;

use crate::{
    AllowAllQueryAuthorizer, Arc, BlockIndex, BlockStoreError, ByteSize, ColdObjectStoreState,
    CompactionFrontier, CompactionFrontierSource, DynamicIndexCache, DynamicIndexCacheKey,
    DynamicIndexSource, DynamicShardIndexCacheKey, DynamicShardRangesCacheKey, HotTailState,
    Instant, LabelIndex, LogHotTail, LogQueryAuthorizer, NonZeroUsize, ObjectPath, ObjectStore,
    PathBuf, QuerierIndexSource, QuerierState, ServiceConfig, ServiceConfigError, ServiceMetrics,
    SharedCompactionFrontier, SharedLogDeleteRequests, SharedLokiRules,
    SharedPrometheusAlertStates, Time, TimeRange, merge_tenant_shard_indexes,
    querier_object_store_inputs, read_log_index_manifest,
    read_tenant_log_index_manifest_from_object_store,
    read_tenant_log_index_shard_from_object_store,
    read_tenant_log_index_shard_ranges_from_object_store,
    read_tenant_log_index_shards_from_object_store,
};

use futures_util::{StreamExt as _, TryStreamExt as _};

// === split-modules: generated submodules ===
mod build_querier_state;
mod build_querier_state_with_object_store_prefix;
mod querier_state;

pub use build_querier_state::build_querier_state;
pub (crate) use build_querier_state_with_object_store_prefix::build_querier_state_with_object_store_prefix;
