use super::{async_trait, CompactionPartitionOffset, CompactionCommitError};

/// Commits compacted WAL offsets after block and index writes are durable.
#[async_trait]
pub trait CompactionOffsetCommitter: Send + Sync {
    async fn commit_offsets(
        &self,
        offsets: &[CompactionPartitionOffset],
    ) -> Result<(), CompactionCommitError>;
}
