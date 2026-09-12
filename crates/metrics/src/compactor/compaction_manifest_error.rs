use super::CompactionIndexError;

/// Errors raised while reading the `.index` manifests under the metrics prefix.
///
/// The manifests are the metrics index, so a pass that cannot read them knows
/// nothing about which blocks are live. Every caller stops rather than acts on
/// a partial view.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CompactionManifestError {
    #[error("compaction manifest object-store operation failed: {0}")]
    ObjectStore(String),

    #[error("compaction manifest key mismatch: listed `{listed}`, manifest `{manifest}`")]
    KeyMismatch { listed: String, manifest: String },

    #[error(transparent)]
    Index(#[from] CompactionIndexError),
}
