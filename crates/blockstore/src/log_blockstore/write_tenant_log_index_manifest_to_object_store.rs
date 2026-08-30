use super::{
    BlockIndex, BlockStoreError, LabelIndex, LogIndexManifest, ObjectPath, ObjectStore,
    ObjectStoreExt, instrument, log_tenant_index_manifest_object_path,
};

#[instrument(skip_all, fields(tenant = %tenant), err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn write_tenant_log_index_manifest_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
) -> Result<(), BlockStoreError> {
    let manifest = LogIndexManifest::from_indexes_for_tenant(tenant, label_index, block_index);
    let payload = serde_json::to_vec_pretty(&manifest)?;
    store
        .put(
            &log_tenant_index_manifest_object_path(prefix, tenant),
            payload.into(),
        )
        .await?;
    Ok(())
}
