use super::{CompactionConsumerCommitError, async_trait};

/// Minimal mutable consumer commit surface for service loops that poll and commit
/// through the same handle.
#[async_trait]
pub trait CompactionConsumerCommitMut: Send {
    async fn commit_sync_mut(&mut self) -> Result<(), CompactionConsumerCommitError>;
}
