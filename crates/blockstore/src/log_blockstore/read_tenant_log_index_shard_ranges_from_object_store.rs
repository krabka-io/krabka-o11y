use super::*;

#[instrument(level = "debug", skip_all, fields(tenant = %tenant), err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_tenant_log_index_shard_ranges_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
) -> Result<Vec<TimeRange>, BlockStoreError> {
    let bytes = store
        .get(&log_tenant_index_shard_catalog_object_path(prefix, tenant))
        .await?
        .bytes()
        .await?;
    let catalog: LogIndexShardCatalog = serde_json::from_slice(&bytes)?;
    catalog.into_shards()
}
