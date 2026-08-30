use super::*;

#[instrument(
    skip_all,
    fields(tenant = %tenant, start_ns = shard_range.start_ns, end_ns = shard_range.end_ns),
    err
)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn write_tenant_log_index_shard_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    shard_range: TimeRange,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
) -> Result<(), BlockStoreError> {
    let manifest = LogIndexManifest::from_indexes_for_tenant_shard(
        tenant,
        shard_range,
        label_index,
        block_index,
    );
    let payload = serde_json::to_vec_pretty(&manifest)?;
    store
        .put(
            &log_tenant_index_shard_manifest_object_path(prefix, tenant, shard_range),
            payload.into(),
        )
        .await?;
    Ok(())
}
