use super::{WalConsumerPoll, Consumer, Time, ConsumerRecord, TracesError, WalConsumerCommit};

#[async_trait::async_trait]
impl WalConsumerPoll for Consumer {
    async fn poll(&mut self, window: Time) -> Result<Vec<ConsumerRecord>, TracesError> {
        Consumer::poll(self, window)
            .await
            .map_err(|err| TracesError::Wal(err.to_string()))
    }
}

#[async_trait::async_trait]
impl WalConsumerCommit for Consumer {
    async fn commit_sync(&mut self) -> Result<(), TracesError> {
        Consumer::commit_sync(self)
            .await
            .map_err(|err| TracesError::Wal(err.to_string()))
    }
}
