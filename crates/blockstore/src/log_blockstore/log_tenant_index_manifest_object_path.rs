use super::{ObjectPath, log_index_manifest_object_path};

#[must_use]
pub fn log_tenant_index_manifest_object_path(prefix: &ObjectPath, tenant: &str) -> ObjectPath {
    log_index_manifest_object_path(&prefix.clone().join(format!("tenant={tenant}")))
}
