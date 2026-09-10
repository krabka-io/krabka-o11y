use super::{CompactionConsumerCommitError, CompactionPartitionOffset, async_trait};

/// Minimal mutable consumer commit surface for service loops that poll and commit
/// through the same handle.
#[async_trait]
pub trait CompactionConsumerCommitMut: Send {
    async fn commit_offsets_sync_mut(
        &mut self,
        offsets: &[CompactionPartitionOffset],
    ) -> Result<(), CompactionConsumerCommitError>;
}
