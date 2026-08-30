use super::*;

#[async_trait::async_trait]
impl WalHeadConsumerPoll for Consumer {
    async fn poll(&mut self, timeout: Time) -> Result<Vec<ConsumerRecord>, WalHeadConsumerError> {
        Consumer::poll(self, timeout)
            .await
            .map_err(|error| WalHeadConsumerError::Poll(error.to_string()))
    }
}

#[async_trait::async_trait]
impl WalHeadConsumerCommit for Consumer {
    async fn commit_sync(&mut self) -> Result<(), WalHeadConsumerError> {
        Consumer::commit_sync(self)
            .await
            .map_err(|error| WalHeadConsumerError::Commit(error.to_string()))
    }
}
