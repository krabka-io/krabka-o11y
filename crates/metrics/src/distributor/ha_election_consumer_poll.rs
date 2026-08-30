use super::*;

#[async_trait::async_trait]
pub trait HaElectionConsumerPoll: Send {
    async fn poll(&mut self, timeout: Time)
    -> Result<Vec<ConsumerRecord>, HaElectionConsumerError>;
}
