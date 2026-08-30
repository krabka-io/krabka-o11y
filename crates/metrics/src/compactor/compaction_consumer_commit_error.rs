
/// Errors raised by a consumer offset commit.
#[derive(Debug, thiserror::Error)]
pub enum CompactionConsumerCommitError {
    #[error("consumer commit failed: {0}")]
    Commit(String),
}
