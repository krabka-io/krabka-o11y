use super::{Consumer, ConsumerRecord, Future, HaElectionConsumerCommit, HaElectionConsumerError, HaElectionConsumerPoll, Time};

#[async_trait::async_trait]
impl HaElectionConsumerPoll for Consumer {
    async fn poll(
        &mut self,
        timeout: Time,
    ) -> Result<Vec<ConsumerRecord>, HaElectionConsumerError> {
        Consumer::poll(self, timeout)
            .await
            .map_err(|error| HaElectionConsumerError::Poll(error.to_string()))
    }
}

#[async_trait::async_trait]
impl HaElectionConsumerCommit for Consumer {
    async fn commit_sync(&mut self) -> Result<(), HaElectionConsumerError> {
        Consumer::commit_sync(self)
            .await
            .map_err(|error| HaElectionConsumerError::Commit(error.to_string()))
    }
}
