use super::{
    CompactionConsumerCommit, CompactionConsumerCommitError, CompactionConsumerPoll,
    CompactionConsumerPollError, Consumer, ConsumerRecord, Time, async_trait,
};

#[async_trait]
impl CompactionConsumerPoll for Consumer {
    async fn poll(
        &mut self,
        timeout: Time,
    ) -> Result<Vec<ConsumerRecord>, CompactionConsumerPollError> {
        Consumer::poll(self, timeout)
            .await
            .map_err(|error| CompactionConsumerPollError::Poll(error.to_string()))
    }
}

#[async_trait]
impl CompactionConsumerCommit for Consumer {
    async fn commit_offsets_sync(
        &self,
        topic: &str,
        offsets: &[super::CompactionPartitionOffset],
    ) -> Result<(), CompactionConsumerCommitError> {
        let offsets = offsets
            .iter()
            .map(|offset| ((topic.to_string(), offset.partition.0), offset.offset.0))
            .collect();
        Consumer::commit_offsets_sync(self, offsets)
            .await
            .map_err(|error| CompactionConsumerCommitError::Commit(error.to_string()))
    }
}
