use super::*;

#[instrument(level = "debug", skip_all, fields(tenant = %tenant), err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn list_tenant_log_index_shard_ranges_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
) -> Result<Vec<TimeRange>, BlockStoreError> {
    let shard_prefix = log_tenant_index_shards_object_prefix(prefix, tenant);
    collect_tenant_log_index_shard_ranges(
        shard_prefix.clone(),
        store.list(Some(&shard_prefix)),
        None,
    )
    .await
}
