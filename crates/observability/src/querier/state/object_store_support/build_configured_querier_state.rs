use super::{
    Arc, BlockIndex, ConfiguredObjectStore, LabelIndex, QuerierIndexSource, QuerierState,
    ServiceConfig, ServiceConfigError, build_querier_state_with_object_store_prefix,
    querier_object_store_prefix,
};

pub(crate) async fn build_configured_querier_state(
    config: &ServiceConfig,
    configured_store: &ConfiguredObjectStore,
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
        let state = QuerierState::new(
            config.data_root.clone(),
            LabelIndex::default(),
            BlockIndex::default(),
        );
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
    )
    .await
}
