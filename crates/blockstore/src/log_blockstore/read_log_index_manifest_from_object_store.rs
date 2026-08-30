use super::*;

#[instrument(level = "debug", skip_all, err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_log_index_manifest_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
    let bytes = store
        .get(&log_index_manifest_object_path(prefix))
        .await?
        .bytes()
        .await?;
    let manifest: LogIndexManifest = serde_json::from_slice(&bytes)?;
    manifest.into_indexes()
}
