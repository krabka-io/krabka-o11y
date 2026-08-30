use super::{async_trait, CompactionConsumerCommitError};

/// Minimal consumer commit surface needed by the compactor loop.
#[async_trait]
pub trait CompactionConsumerCommit: Send + Sync {
    async fn commit_sync(&self) -> Result<(), CompactionConsumerCommitError>;
}
