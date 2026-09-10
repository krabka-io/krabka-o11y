use super::{
    CompactionConsumerCommit, CompactionConsumerCommitError, CompactionConsumerCommitMut,
    CompactionConsumerPoll, CompactionConsumerPollError, CompactionPartitionOffset, ConsumerRecord,
    Time, async_trait,
};

/// Polling consumer that recommits every highest durable partition offset.
pub struct DurableCompactionConsumer<C> {
    inner: C,
    topic: String,
}

impl<C> DurableCompactionConsumer<C> {
    #[must_use]
    pub fn new(inner: C, topic: impl Into<String>) -> Self {
        Self {
            inner,
            topic: topic.into(),
        }
    }
}

#[async_trait]
impl<C> CompactionConsumerPoll for DurableCompactionConsumer<C>
where
    C: CompactionConsumerPoll + Send,
{
    async fn poll(
        &mut self,
        timeout: Time,
    ) -> Result<Vec<ConsumerRecord>, CompactionConsumerPollError> {
        self.inner.poll(timeout).await
    }
}

#[async_trait]
impl<C> CompactionConsumerCommitMut for DurableCompactionConsumer<C>
where
    C: CompactionConsumerCommit + Send,
{
    async fn commit_offsets_sync_mut(
        &mut self,
        offsets: &[CompactionPartitionOffset],
    ) -> Result<(), CompactionConsumerCommitError> {
        self.inner.commit_offsets_sync(&self.topic, offsets).await
    }
}
