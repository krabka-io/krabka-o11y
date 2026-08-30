use super::*;

#[must_use]
pub fn log_tenant_index_shard_list_offset_object_path(
    prefix: &ObjectPath,
    tenant: &str,
    query_range: TimeRange,
) -> ObjectPath {
    log_tenant_index_shards_object_prefix(prefix, tenant).join(format!(
        "time={}",
        log_tenant_index_shard_list_offset_start_ns(query_range)
    ))
}
