use super::*;

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
