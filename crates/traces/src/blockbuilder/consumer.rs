use krabka_observability::{
    wal_consumer_metrics::WalConsumerMetrics, wal_group_assignment::WalAssignmentWatch,
};

use super::{Consumer, ConsumerRecord, Time, TracesError, WalConsumerCommit, WalConsumerPoll};

/// The WAL consumer the traces block builder drives.
///
/// It is the `krabka-client-consumer` [`Consumer`] and one
/// [`WalAssignmentWatch`] over it. The watch runs on every poll, so a rebalance
/// that takes partitions away from this member reaches the instruments and the
/// log instead of passing unseen.
///
/// `run` needs the watch here rather than in its own loop because the loop is
/// generic over [`WalConsumerPoll`], and a test drives it with a scripted fake
/// that joins no group.
///
/// The watch reports the rebalance. It cannot repair it: the block builder
/// holds decoded windows across polls, and the group releases a partition with
/// no callback into this process. See
/// [`krabka_observability::wal_group_assignment`].
pub struct BlockBuilderConsumer {
    consumer: Consumer,
    assignment: WalAssignmentWatch,
}

impl BlockBuilderConsumer {
    /// Wraps `consumer` and reports its assignment through `metrics`.
    #[must_use]
    pub fn new(consumer: Consumer, metrics: &WalConsumerMetrics) -> Self {
        Self {
            consumer,
            assignment: WalAssignmentWatch::new(metrics.clone()),
        }
    }
}

#[async_trait::async_trait]
impl WalConsumerPoll for BlockBuilderConsumer {
    async fn poll(&mut self, window: Time) -> Result<Vec<ConsumerRecord>, TracesError> {
        let records = Consumer::poll(&mut self.consumer, window)
            .await
            .map_err(|err| TracesError::Wal(err.to_string()))?;
        // Read after the poll, so the snapshot is the one the fetch was served
        // against. An empty poll is observed too: a member that lost every
        // partition returns nothing and would otherwise look idle.
        self.assignment.observe_consumer(&self.consumer).await;
        Ok(records)
    }
}

#[async_trait::async_trait]
impl WalConsumerCommit for BlockBuilderConsumer {
    async fn commit_sync(&mut self) -> Result<(), TracesError> {
        Consumer::commit_sync(&self.consumer)
            .await
            .map_err(|err| TracesError::Wal(err.to_string()))
    }
}
