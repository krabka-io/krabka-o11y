use krabka_observability::{
    wal_consumer_metrics::WalConsumerMetrics, wal_group_assignment::WalAssignmentWatch,
};

use super::{
    CompactionConsumerCommit, CompactionConsumerCommitError, CompactionConsumerPoll,
    CompactionConsumerPollError, Consumer, ConsumerRecord, Time, async_trait,
};

/// The WAL consumer the metrics compactor drives.
///
/// It is the `krabka-client-consumer` [`Consumer`] and one
/// [`WalAssignmentWatch`] over it. The watch runs on every poll, so a rebalance
/// that takes partitions away from this member reaches the instruments and the
/// log instead of passing unseen.
///
/// The compaction loop buffers WAL records across polls and commits only the
/// offsets its durable writes produced. That discipline holds while this member
/// keeps its partitions. It cannot survive a partition that moves: the group
/// releases the partition with no callback into this process, so the buffered
/// records for it are abandoned. See
/// [`krabka_observability::wal_group_assignment`].
pub struct WalAssignmentConsumer {
    consumer: Consumer,
    assignment: WalAssignmentWatch,
}

impl WalAssignmentConsumer {
    /// Wraps `consumer` and reports its assignment through `metrics`.
    #[must_use]
    pub fn new(consumer: Consumer, metrics: &WalConsumerMetrics) -> Self {
        Self {
            consumer,
            assignment: WalAssignmentWatch::new(metrics.clone()),
        }
    }
}

#[async_trait]
impl CompactionConsumerPoll for WalAssignmentConsumer {
    async fn poll(
        &mut self,
        timeout: Time,
    ) -> Result<Vec<ConsumerRecord>, CompactionConsumerPollError> {
        let records = Consumer::poll(&mut self.consumer, timeout)
            .await
            .map_err(|error| CompactionConsumerPollError::Poll(error.to_string()))?;
        // Read after the poll, so the snapshot is the one the fetch was served
        // against. An empty poll is observed too: a member that lost every
        // partition returns nothing and would otherwise look idle.
        self.assignment.observe_consumer(&self.consumer).await;
        Ok(records)
    }
}

#[async_trait]
impl CompactionConsumerCommit for WalAssignmentConsumer {
    async fn commit_offsets_sync(
        &self,
        topic: &str,
        offsets: &[super::CompactionPartitionOffset],
    ) -> Result<(), CompactionConsumerCommitError> {
        let offsets = offsets
            .iter()
            .map(|offset| ((topic.to_string(), offset.partition.0), offset.offset.0))
            .collect();
        Consumer::commit_offsets_sync(&self.consumer, offsets)
            .await
            .map_err(|error| CompactionConsumerCommitError::Commit(error.to_string()))
    }
}
