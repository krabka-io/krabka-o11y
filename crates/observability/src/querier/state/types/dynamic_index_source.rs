use super::{Arc, ObjectPath, ObjectStore};

#[derive(Clone)]
pub(crate) enum DynamicIndexSource {
    TenantObjectStoreManifest {
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
    },
    TenantObjectStoreShards {
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
    },
}
