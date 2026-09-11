use super::{ObjectPath, escape_object_path_segment};

#[must_use]
pub fn log_tenant_index_shards_object_prefix(prefix: &ObjectPath, tenant: &str) -> ObjectPath {
    prefix
        .clone()
        .join(format!("tenant={}", escape_object_path_segment(tenant)))
        .join("index")
        .join("logs")
        .join("shards")
}
