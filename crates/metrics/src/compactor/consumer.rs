use super::{
    CompactionConsumerCommit, CompactionConsumerCommitError, CompactionConsumerCommitMut,
    CompactionConsumerPoll, CompactionConsumerPollError, Consumer, ConsumerRecord, Time, async_trait};

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
    async fn commit_sync(&self) -> Result<(), CompactionConsumerCommitError> {
        Consumer::commit_sync(self)
            .await
            .map_err(|error| CompactionConsumerCommitError::Commit(error.to_string()))
    }
}

#[async_trait]
impl CompactionConsumerCommitMut for Consumer {
    async fn commit_sync_mut(&mut self) -> Result<(), CompactionConsumerCommitError> {
        Consumer::commit_sync(self)
            .await
            .map_err(|error| CompactionConsumerCommitError::Commit(error.to_string()))
    }
}
