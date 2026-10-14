use super::{
    ObjectPath, ObjectStore, ServiceConfig, ServiceConfigError, querier_object_store_prefix,
};

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
