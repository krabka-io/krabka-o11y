use crate::{
    Arc, BlockIndex, ConfiguredObjectStore, LabelIndex, ObjectPath, ObjectStore, OverridesProvider,
    QuerierIndexSource, QuerierState, ServiceConfig, ServiceConfigError,
    build_querier_state_with_object_store_prefix,
};

pub(crate) fn effective_object_store_prefix(
    base: Option<&ObjectPath>,
    index_prefix: &str,
) -> ObjectPath {
    let index_prefix = index_prefix.trim_matches('/');
    let Some(base) = base else {
        return ObjectPath::from(index_prefix);
    };
    let base = base.as_ref().trim_matches('/');

    if base.is_empty() {
        if index_prefix.is_empty() {
            ObjectPath::from("")
        } else {
            ObjectPath::from(index_prefix)
        }
    } else if index_prefix.is_empty() {
        ObjectPath::from(base)
    } else {
        ObjectPath::from(format!("{base}/{index_prefix}"))
    }
}

pub(crate) fn querier_object_store_prefix(
    config: &ServiceConfig,
    object_store_prefix: Option<&ObjectPath>,
) -> Result<Option<ObjectPath>, ServiceConfigError> {
    match config.querier_index_source {
        QuerierIndexSource::LocalManifest => Ok(None),
        QuerierIndexSource::TenantObjectStoreManifest
        | QuerierIndexSource::TenantObjectStoreShards => {
            let prefix =
                config
                    .index_prefix
                    .as_deref()
                    .ok_or(ServiceConfigError::MissingIndexPrefix {
                        index_source: config.querier_index_source,
                    })?;
            Ok(Some(effective_object_store_prefix(
                object_store_prefix,
                prefix,
            )))
        }
    }
}

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

pub(crate) struct QuerierObjectStoreInputs<'a> {
    pub(crate) store: &'a dyn ObjectStore,
    pub(crate) tenant: &'a str,
    pub(crate) prefix: ObjectPath,
}

pub(crate) fn querier_object_store_inputs<'a>(
    config: &'a ServiceConfig,
    object_store: Option<&'a dyn ObjectStore>,
    object_store_prefix: Option<&ObjectPath>,
) -> Result<QuerierObjectStoreInputs<'a>, ServiceConfigError> {
    let store = object_store.ok_or(ServiceConfigError::MissingObjectStore)?;
    let tenant = config
        .tenant
        .as_deref()
        .ok_or(ServiceConfigError::MissingTenant {
            index_source: config.querier_index_source,
        })?;
    let prefix = querier_object_store_prefix(config, object_store_prefix)?.ok_or(
        ServiceConfigError::MissingIndexPrefix {
            index_source: config.querier_index_source,
        },
    )?;

    Ok(QuerierObjectStoreInputs {
        store,
        tenant,
        prefix,
    })
}
