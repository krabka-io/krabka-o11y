
/// Errors raised while committing compactor WAL offsets.
#[derive(Debug, thiserror::Error)]
pub enum CompactionCommitError {
    #[error("compaction offset commit failed: {0}")]
    Commit(String),
}
