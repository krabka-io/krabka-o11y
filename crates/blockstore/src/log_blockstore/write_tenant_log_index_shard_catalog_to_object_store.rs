use super::*;

#[instrument(skip_all, fields(tenant = %tenant, shards = shard_ranges.len()), err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn write_tenant_log_index_shard_catalog_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    shard_ranges: &[TimeRange],
) -> Result<(), BlockStoreError> {
    let catalog = LogIndexShardCatalog::new(shard_ranges);
    let payload = serde_json::to_vec_pretty(&catalog)?;
    store
        .put(
            &log_tenant_index_shard_catalog_object_path(prefix, tenant),
            payload.into(),
        )
        .await?;
    Ok(())
}
