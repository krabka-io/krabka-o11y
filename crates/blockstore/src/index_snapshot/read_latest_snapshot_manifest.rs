use super::{
    Arc, ByteSize, IndexSnapshotBytes, ObjectStore, Result, SnapshotManifest,
    latest_index_snapshot_path, read_index_snapshot_bytes,
};

/// The manifest a reader should answer from, or `None` when the key has no
/// generation yet.
///
/// # Errors
/// Returns an error when object-store I/O fails or the manifest is malformed.
pub(crate) async fn read_latest_snapshot_manifest(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    max_bytes: ByteSize,
    label: &str,
) -> Result<Option<SnapshotManifest>> {
    let Some(path) = latest_index_snapshot_path(store, key).await? else {
        return Ok(None);
    };
    match read_index_snapshot_bytes(store, &path, max_bytes, label).await? {
        IndexSnapshotBytes::Present(bytes) => {
            Ok(Some(SnapshotManifest::from_bytes(label, &bytes)?))
        }
        IndexSnapshotBytes::Absent(missing) => Err(missing),
    }
}
