use super::{
    CompactionCommitError, CompactionConsumerCommit, CompactionOffsetCommitter,
    CompactionPartitionOffset, async_trait,
};

/// Adapter that commits the underlying consumer after durable compaction writes.
pub struct CompactionConsumerCommitter<'a, C: ?Sized> {
    pub(crate) consumer: &'a C,
    pub(crate) topic: &'a str,
}

impl<'a, C: ?Sized> CompactionConsumerCommitter<'a, C> {
    #[must_use]
    pub const fn new(consumer: &'a C, topic: &'a str) -> Self {
        Self { consumer, topic }
    }
}

#[async_trait]
impl<C> CompactionOffsetCommitter for CompactionConsumerCommitter<'_, C>
where
    C: CompactionConsumerCommit + ?Sized,
{
    async fn commit_offsets(
        &self,
        offsets: &[CompactionPartitionOffset],
    ) -> Result<(), CompactionCommitError> {
        if offsets.is_empty() {
            return Ok(());
        }
        self.consumer
            .commit_offsets_sync(self.topic, offsets)
            .await
            .map_err(|error| CompactionCommitError::Commit(error.to_string()))
    }
}
