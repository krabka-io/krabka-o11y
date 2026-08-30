use super::*;

#[instrument(skip_all, err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn write_log_index_manifest_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
) -> Result<(), BlockStoreError> {
    let manifest = LogIndexManifest::from_indexes(label_index, block_index);
    let payload = serde_json::to_vec_pretty(&manifest)?;
    store
        .put(&log_index_manifest_object_path(prefix), payload.into())
        .await?;
    Ok(())
}
