use super::*;

#[instrument(level = "debug", skip_all, err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn read_log_index_manifest(
    root: impl AsRef<Path>,
) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
    let manifest: LogIndexManifest =
        serde_json::from_reader(File::open(log_index_manifest_path(root))?)?;
    manifest.into_indexes()
}
