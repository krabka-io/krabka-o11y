use super::{
    BlockStoreError, ObjectPath, ObjectStore, TimeRange, collect_tenant_log_index_shard_ranges,
    instrument, log_tenant_index_shard_list_offset_object_path,
    log_tenant_index_shards_object_prefix,
};

#[instrument(
    level = "debug",
    skip_all,
    fields(tenant = %tenant, start_ns = query_range.start_ns, end_ns = query_range.end_ns),
    err
)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn list_tenant_log_index_shard_ranges_overlapping_query_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    query_range: TimeRange,
) -> Result<Vec<TimeRange>, BlockStoreError> {
    let shard_prefix = log_tenant_index_shards_object_prefix(prefix, tenant);
    let offset = log_tenant_index_shard_list_offset_object_path(prefix, tenant, query_range);
    collect_tenant_log_index_shard_ranges(
        shard_prefix,
        store.list_with_offset(
            Some(&log_tenant_index_shards_object_prefix(prefix, tenant)),
            &offset,
        ),
        Some(query_range),
    )
    .await
}
