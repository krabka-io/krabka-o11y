use super::*;

#[must_use]
pub fn log_tenant_index_shard_catalog_object_path(prefix: &ObjectPath, tenant: &str) -> ObjectPath {
    log_tenant_index_shards_object_prefix(prefix, tenant).join("manifest.json")
}
