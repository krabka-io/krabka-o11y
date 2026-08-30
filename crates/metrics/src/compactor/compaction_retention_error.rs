use super::CompactionIndexError;

/// Errors raised while deleting compacted metric objects outside retention.
#[derive(Debug, thiserror::Error)]
pub enum CompactionRetentionError {
    #[error("compaction retention object-store operation failed: {0}")]
    ObjectStore(String),

    #[error("compaction retention manifest key mismatch: listed `{listed}`, manifest `{manifest}`")]
    ManifestKeyMismatch { listed: String, manifest: String },

    #[error(transparent)]
    Index(#[from] CompactionIndexError),
}
