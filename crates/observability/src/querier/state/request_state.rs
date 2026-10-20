use futures_util::{StreamExt as _, TryStreamExt as _};
use krabka_units::convert::{ByteSizeExt, StdDurationExt, TimeExt};

use super::types::TenantIndexPair;
use crate::{
    AllowAllQueryAuthorizer, Arc, BlockIndex, BlockStoreError, ColdObjectStoreState,
    CompactionFrontier, CompactionFrontierSource, DynamicIndexCache, DynamicIndexCacheKey,
    DynamicIndexSource, DynamicShardIndexCacheKey, DynamicShardRangesCacheKey, ExecutionOptions,
    HotTailState, InMemoryCache, Instant, LabelIndex, Limits, LogHotTail, LogQueryAuthorizer,
    NonZeroUsize, ObjectPath, ObjectStore, OverridesProvider, PathBuf, QuerierIndexSource,
    QuerierState, ServiceConfig, ServiceConfigError, ServiceMetrics, SharedCompactionFrontier,
    SharedLogDeleteRequests, SharedLokiRules, SharedPrometheusAlertStates, TenantId, TimeRange,
    Value, days, hours, limits_provider_for_config, merge_tenant_shard_indexes,
    querier_object_store_inputs, read_log_index_manifest,
    read_tenant_log_index_manifest_from_object_store,
    read_tenant_log_index_shard_from_object_store,
    read_tenant_log_index_shard_ranges_or_empty_from_object_store,
    read_tenant_log_index_shards_from_object_store,
};

mod build_querier_state;
mod querier_state;

pub use build_querier_state::build_querier_state;
pub(crate) use build_querier_state::build_querier_state_with_overrides;

pub(crate) async fn build_querier_state_with_object_store_prefix(
    config: &ServiceConfig,
    object_store: Option<&dyn ObjectStore>,
    object_store_prefix: Option<&ObjectPath>,
    overrides: Arc<OverridesProvider>,
) -> Result<QuerierState, ServiceConfigError> {
    let state = match config.querier_index_source {
        QuerierIndexSource::LocalManifest => QuerierState::from_manifest(config.data_root.clone())?,
        QuerierIndexSource::TenantObjectStoreManifest => {
            let inputs = querier_object_store_inputs(config, object_store, object_store_prefix)?;
            QuerierState::from_tenant_object_store(
                config.data_root.clone(),
                inputs.store,
                &inputs.prefix,
                inputs.tenant,
            )
            .await?
        }
        QuerierIndexSource::TenantObjectStoreShards => {
            let inputs = querier_object_store_inputs(config, object_store, object_store_prefix)?;
            let start_ns = config
                .query_start_ns
                .ok_or(ServiceConfigError::MissingQueryStartNs)?;
            let end_ns = config
                .query_end_ns
                .ok_or(ServiceConfigError::MissingQueryEndNs)?;

            QuerierState::from_tenant_object_store_shards(
                config.data_root.clone(),
                inputs.store,
                &inputs.prefix,
                inputs.tenant,
                TimeRange::new(start_ns, end_ns)?,
            )
            .await?
        }
    };

    Ok(state
        .with_runtime_policy(config)
        .with_limits_overrides_source(overrides))
}
