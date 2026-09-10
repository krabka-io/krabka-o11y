use super::SnapshotManifest;

/// The state a snapshot write starts from: the manifest the newest generation
/// published, and the generation the write should claim next.
pub(crate) struct ManifestSnapshotBase {
    /// Newest stored manifest, or `None` when the key has no generation yet.
    pub(crate) manifest: Option<SnapshotManifest>,
    /// Generation to write, one past the newest stored one.
    pub(crate) next_generation: u64,
}
