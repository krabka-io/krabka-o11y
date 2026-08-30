use super::{CompactionConsumerPollError, ConsumerRecord, Time, async_trait};

/// Minimal consumer poll surface needed by the compactor loop.
#[async_trait]
pub trait CompactionConsumerPoll: Send {
    async fn poll(
        &mut self,
        timeout: Time,
    ) -> Result<Vec<ConsumerRecord>, CompactionConsumerPollError>;
}
