use super::{
    ConfiguredObjectStore, ObjectPath, ObjectStore, ServiceConfig, ServiceConfigError,
    effective_object_store_prefix,
};

/// Resolves the store a compactor writes to, and the index prefix under it.
///
/// An injected `object_store` wins over the configured one, and then the
/// configured store's own prefix does not apply.
pub(crate) fn compactor_object_store<'a>(
    config: &ServiceConfig,
    object_store: Option<&'a dyn ObjectStore>,
    configured_store: Option<&'a ConfiguredObjectStore>,
) -> Result<(&'a dyn ObjectStore, ObjectPath), ServiceConfigError> {
    let (store, object_store_prefix) = if let Some(store) = object_store {
        (store, None)
    } else {
        let configured_store = configured_store.ok_or(ServiceConfigError::MissingObjectStore)?;
        (
            configured_store.store.as_ref(),
            Some(&configured_store.prefix),
        )
    };
    let index_prefix = config
        .index_prefix
        .as_deref()
        .ok_or(ServiceConfigError::MissingCompactorIndexPrefix)?;
    Ok((
        store,
        effective_object_store_prefix(object_store_prefix, index_prefix),
    ))
}
