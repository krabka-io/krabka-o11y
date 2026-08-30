use super::ObjectPath;

#[must_use]
pub fn log_tenant_index_shards_object_prefix(prefix: &ObjectPath, tenant: &str) -> ObjectPath {
    prefix
        .clone()
        .join(format!("tenant={tenant}"))
        .join("index")
        .join("logs")
        .join("shards")
}
