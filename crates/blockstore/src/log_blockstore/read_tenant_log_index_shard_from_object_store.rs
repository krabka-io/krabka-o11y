use super::{
    BlockIndex, BlockStoreError, LabelIndex, LogIndexManifest, ObjectPath, ObjectStore,
    ObjectStoreExt, TimeRange, instrument, log_tenant_index_shard_manifest_object_path,
};

#[instrument(
    level = "debug",
    skip_all,
    fields(tenant = %tenant, start_ns = shard_range.start_ns, end_ns = shard_range.end_ns),
    err
)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_tenant_log_index_shard_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    shard_range: TimeRange,
) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
    let bytes = store
        .get(&log_tenant_index_shard_manifest_object_path(
            prefix,
            tenant,
            shard_range,
        ))
        .await?
        .bytes()
        .await?;
    let manifest: LogIndexManifest = serde_json::from_slice(&bytes)?;
    manifest.into_indexes_for_tenant(tenant)
}
