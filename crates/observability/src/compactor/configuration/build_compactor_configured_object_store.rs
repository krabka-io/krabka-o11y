use krabka_blockstore::ObjectStoreMetrics;

use super::{
    ConfiguredObjectStore, ObjectStore, ServiceConfig, ServiceConfigError,
    build_configured_object_store,
};

pub(crate) fn build_compactor_configured_object_store(
    config: &ServiceConfig,
    object_store: Option<&dyn ObjectStore>,
    metrics: ObjectStoreMetrics,
) -> Result<Option<ConfiguredObjectStore>, ServiceConfigError> {
    if object_store.is_some() {
        return Ok(None);
    }

    build_configured_object_store(config, metrics)
}
