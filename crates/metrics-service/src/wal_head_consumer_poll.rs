use super::{ConsumerRecord, Time, WalHeadConsumerError};

#[async_trait::async_trait]
pub trait WalHeadConsumerPoll: Send {
    async fn poll(&mut self, timeout: Time) -> Result<Vec<ConsumerRecord>, WalHeadConsumerError>;
}
