use super::{CompactionConsumerCommitError, CompactionPartitionOffset, async_trait};

/// Minimal consumer commit surface needed by the compactor loop.
#[async_trait]
pub trait CompactionConsumerCommit: Send + Sync {
    async fn commit_offsets_sync(
        &self,
        topic: &str,
        offsets: &[CompactionPartitionOffset],
    ) -> Result<(), CompactionConsumerCommitError>;
}
