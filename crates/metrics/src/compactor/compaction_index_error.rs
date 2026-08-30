
/// Compaction index sidecar codec errors.
#[derive(Debug, thiserror::Error)]
pub enum CompactionIndexError {
    #[error("compaction index encode failed: {0}")]
    Encode(String),

    #[error("compaction index decode failed: {0}")]
    Decode(String),

    #[error("compaction index object-store write failed: {0}")]
    ObjectStore(String),
}
