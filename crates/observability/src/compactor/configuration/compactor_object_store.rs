use super::{
    ConfiguredObjectStore, ObjectPath, ObjectStore, ServiceConfig, ServiceConfigError,
    effective_object_store_prefix,
};

pub(crate) struct CompactorObjectStore<'a> {
    pub(crate) store: &'a dyn ObjectStore,
    pub(crate) prefix: ObjectPath,
}

struct ObjectStoreSelection<'a> {
    store: &'a dyn ObjectStore,
    configured_prefix: Option<&'a ObjectPath>,
}

/// Resolves the store a compactor writes to, and the index prefix under it.
///
/// An injected `object_store` wins over the configured one, and then the
/// configured store's own prefix does not apply.
pub(crate) fn compactor_object_store<'a>(
    config: &ServiceConfig,
    object_store: Option<&'a dyn ObjectStore>,
    configured_store: Option<&'a ConfiguredObjectStore>,
) -> Result<CompactorObjectStore<'a>, ServiceConfigError> {
    let selection = if let Some(store) = object_store {
        ObjectStoreSelection {
            store,
            configured_prefix: None,
        }
    } else {
        let configured_store = configured_store.ok_or(ServiceConfigError::MissingObjectStore)?;
        ObjectStoreSelection {
            store: configured_store.store.as_ref(),
            configured_prefix: Some(&configured_store.prefix),
        }
    };
    let index_prefix = config
        .index_prefix
        .as_deref()
        .ok_or(ServiceConfigError::MissingCompactorIndexPrefix)?;
    Ok(CompactorObjectStore {
        store: selection.store,
        prefix: effective_object_store_prefix(selection.configured_prefix, index_prefix),
    })
}
