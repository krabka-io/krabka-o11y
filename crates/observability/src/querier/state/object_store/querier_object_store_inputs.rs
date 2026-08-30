use super::*;

pub(crate) fn querier_object_store_inputs<'a>(
    config: &'a ServiceConfig,
    object_store: Option<&'a dyn ObjectStore>,
    object_store_prefix: Option<&ObjectPath>,
) -> Result<(&'a dyn ObjectStore, &'a str, ObjectPath), ServiceConfigError> {
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

    Ok((store, tenant, prefix))
}
