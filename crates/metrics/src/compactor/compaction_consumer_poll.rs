use super::{async_trait, Time, ConsumerRecord, CompactionConsumerPollError};

/// Minimal consumer poll surface needed by the compactor loop.
#[async_trait]
pub trait CompactionConsumerPoll: Send {
    async fn poll(
        &mut self,
        timeout: Time,
    ) -> Result<Vec<ConsumerRecord>, CompactionConsumerPollError>;
}
