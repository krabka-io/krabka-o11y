use super::{
    Arc, ByteSize, Bytes, IndexSnapshotBytes, ObjectStore, Path, Result,
    list_index_snapshot_objects, read_index_snapshot_bytes, snapshot_generation_from_path,
};

/// The state a snapshot write starts from: what the newest stored snapshot
/// holds, and the generation the write should claim next.
pub(crate) struct IndexSnapshotBase {
    /// Serialised newest snapshot, or `None` when the key has none yet.
    pub(crate) bytes: Option<Bytes>,
    /// Generation to write, one past the newest stored one.
    pub(crate) next_generation: u64,
}

/// Resolves the merge base for the next snapshot write.
///
/// `Ok(None)` means the snapshot the listing named was pruned before it could
/// be read, so the chain moved on under us and the caller should look again.
pub(crate) async fn read_index_snapshot_base(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    max_bytes: ByteSize,
    label: &str,
) -> Result<Option<IndexSnapshotBase>> {
    if let Some(meta) = list_index_snapshot_objects(store, key).await?.pop() {
        let generation = snapshot_generation_from_path(&meta.location)?;
        let IndexSnapshotBytes::Present(bytes) =
            read_index_snapshot_bytes(store, &meta.location, max_bytes, label).await?
        else {
            return Ok(None);
        };
        return Ok(Some(IndexSnapshotBase {
            bytes: Some(bytes),
            next_generation: generation.saturating_add(1),
        }));
    }

    // No generation exists yet. A reader falls back to the single object that
    // `save` writes at `key`, so that object is the merge base too, and the
    // first generation is 0.
    let bytes = match read_index_snapshot_bytes(store, &Path::from(key), max_bytes, label).await? {
        IndexSnapshotBytes::Present(bytes) => Some(bytes),
        IndexSnapshotBytes::Absent(_) => None,
    };
    Ok(Some(IndexSnapshotBase {
        bytes,
        next_generation: 0,
    }))
}
