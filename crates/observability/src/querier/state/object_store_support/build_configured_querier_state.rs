use super::{
    Arc, BlockIndex, ConfiguredObjectStore, LabelIndex, OverridesProvider, QuerierIndexSource,
    QuerierState, ServiceConfig, ServiceConfigError, build_querier_state_with_object_store_prefix,
    querier_object_store_prefix,
};

pub(crate) async fn build_configured_querier_state(
    config: &ServiceConfig,
    configured_store: &ConfiguredObjectStore,
    overrides: Arc<OverridesProvider>,
) -> Result<QuerierState, ServiceConfigError> {
    if config.tenant.is_none()
        && matches!(
            config.querier_index_source,
            QuerierIndexSource::TenantObjectStoreManifest
                | QuerierIndexSource::TenantObjectStoreShards
        )
    {
        let prefix = querier_object_store_prefix(config, Some(&configured_store.prefix))?.ok_or(
            ServiceConfigError::MissingIndexPrefix {
                index_source: config.querier_index_source,
            },
        )?;
        // The limits and the runtime policy are applied here too. This branch
        // reads its index per request rather than once at start-up, and a
        // querier that took that branch used to answer with no query limits at
        // all.
        let state = QuerierState::new(
            config.data_root.clone(),
            LabelIndex::default(),
            BlockIndex::default(),
        )
        .with_runtime_policy(config)
        .with_limits_overrides_source(overrides);
        return Ok(match config.querier_index_source {
            QuerierIndexSource::TenantObjectStoreManifest => state
                .with_dynamic_tenant_object_store_manifest(
                    Arc::clone(&configured_store.store),
                    prefix,
                ),
            QuerierIndexSource::TenantObjectStoreShards => state
                .with_dynamic_tenant_object_store_shards(
                    Arc::clone(&configured_store.store),
                    prefix,
                ),
            QuerierIndexSource::LocalManifest => state,
        });
    }

    build_querier_state_with_object_store_prefix(
        config,
        Some(configured_store.store.as_ref()),
        Some(&configured_store.prefix),
        overrides,
    )
    .await
}
