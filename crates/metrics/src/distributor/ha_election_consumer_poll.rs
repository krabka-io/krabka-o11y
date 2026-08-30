use super::{ConsumerRecord, HaElectionConsumerError, Time};

#[async_trait::async_trait]
pub trait HaElectionConsumerPoll: Send {
    async fn poll(&mut self, timeout: Time)
    -> Result<Vec<ConsumerRecord>, HaElectionConsumerError>;
}
