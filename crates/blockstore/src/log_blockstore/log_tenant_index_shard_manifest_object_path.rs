use super::*;

#[must_use]
pub fn log_tenant_index_shard_manifest_object_path(
    prefix: &ObjectPath,
    tenant: &str,
    shard_range: TimeRange,
) -> ObjectPath {
    log_tenant_index_shards_object_prefix(prefix, tenant)
        .join(format!(
            "time={}-{}",
            shard_range.start_ns, shard_range.end_ns
        ))
        .join("manifest.json")
}
