use super::{
    Arc, ByteSize, IndexSnapshotBytes, ManifestSnapshotBase, ObjectStore, Result, SnapshotManifest,
    list_index_snapshot_objects, read_index_snapshot_bytes, snapshot_generation_from_path,
};

/// Resolves the merge base for the next snapshot write.
///
/// `Ok(None)` means the manifest the listing named was pruned before it could
/// be read, so the chain moved on under us and the caller should look again.
pub(crate) async fn read_manifest_snapshot_base(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    max_bytes: ByteSize,
    label: &str,
) -> Result<Option<ManifestSnapshotBase>> {
    let Some(meta) = list_index_snapshot_objects(store, key).await?.pop() else {
        // No generation exists yet. There is no chain and nothing to merge
        // into, and the first generation is zero.
        return Ok(Some(ManifestSnapshotBase {
            manifest: None,
            next_generation: 0,
        }));
    };
    let generation = snapshot_generation_from_path(&meta.location)?;
    let IndexSnapshotBytes::Present(bytes) =
        read_index_snapshot_bytes(store, &meta.location, max_bytes, label).await?
    else {
        return Ok(None);
    };
    Ok(Some(ManifestSnapshotBase {
        manifest: Some(SnapshotManifest::from_bytes(label, &bytes)?),
        next_generation: generation.saturating_add(1),
    }))
}
