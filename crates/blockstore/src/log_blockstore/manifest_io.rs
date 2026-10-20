use super::{
    BlockIndex, BlockStoreError, File, LabelIndex, LogIndexManifest, LogIndexes, ObjectPath,
    ObjectStore, ObjectStoreExt, Path, fs, instrument, log_index_manifest_object_path,
    log_index_manifest_path, log_tenant_index_manifest_object_path,
};

#[instrument(level = "debug", skip_all, err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_log_index_manifest_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> Result<LogIndexes, BlockStoreError> {
    let bytes = store
        .get(&log_index_manifest_object_path(prefix))
        .await?
        .bytes()
        .await?;
    let manifest: LogIndexManifest = serde_json::from_slice(&bytes)?;
    manifest.into_indexes()
}

#[instrument(level = "debug", skip_all, fields(tenant = %tenant), err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_tenant_log_index_manifest_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
) -> Result<LogIndexes, BlockStoreError> {
    let bytes = store
        .get(&log_tenant_index_manifest_object_path(prefix, tenant))
        .await?
        .bytes()
        .await?;
    let manifest: LogIndexManifest = serde_json::from_slice(&bytes)?;
    manifest.into_indexes_for_tenant(tenant)
}

#[instrument(skip_all, err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn write_log_index_manifest(
    root: impl AsRef<Path>,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
) -> Result<(), BlockStoreError> {
    let path = log_index_manifest_path(root);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let manifest = LogIndexManifest::from_indexes(label_index, block_index);
    serde_json::to_writer_pretty(File::create(path)?, &manifest)?;
    Ok(())
}

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
