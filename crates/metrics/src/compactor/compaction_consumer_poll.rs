use super::{CompactionConsumerPollError, ConsumerRecord, Time, async_trait};

/// Minimal consumer poll surface needed by the compactor loop.
#[async_trait]
pub trait CompactionConsumerPoll: Send {
    /// Partitions revoked by the rebalance applied during the last poll.
    fn take_revoked_partitions(&mut self) -> std::collections::BTreeSet<i32> {
        std::collections::BTreeSet::new()
    }

    /// Whether the final durable commits cover a frozen WAL boundary.
    ///
    /// The loop calls this only after it flushes every current buffer on shutdown.
    /// Finite test consumers have no additional records to drain.
    async fn drain_complete(&mut self) -> Result<bool, CompactionConsumerPollError> {
        Ok(true)
    }

    async fn poll(
        &mut self,
        timeout: Time,
    ) -> Result<Vec<ConsumerRecord>, CompactionConsumerPollError>;
}
