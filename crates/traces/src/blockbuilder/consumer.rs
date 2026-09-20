use krabka_observability::{
    ReadinessGate,
    wal_consumer_metrics::WalConsumerMetrics,
    wal_group_assignment::{WalAssignmentWatch, WalRebalanceListener},
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
/// The listener reports revoked partitions to the block builder before it
/// merges records from the new assignment.
pub struct BlockBuilderConsumer {
    consumer: Consumer,
    assignment: WalAssignmentWatch,
    rebalance: WalRebalanceListener,
    metrics: WalConsumerMetrics,
}

impl BlockBuilderConsumer {
    /// Wraps `consumer` and reports its assignment through `metrics`.
    #[must_use]
    pub fn new(consumer: Consumer, metrics: &WalConsumerMetrics) -> Self {
        Self {
            consumer,
            assignment: WalAssignmentWatch::new(metrics.clone()),
            rebalance: WalRebalanceListener::new(""),
            metrics: metrics.clone(),
        }
    }

    #[must_use]
    pub fn with_catch_up(
        consumer: Consumer,
        metrics: &WalConsumerMetrics,
        gate: ReadinessGate,
        rebalance: WalRebalanceListener,
    ) -> Self {
        Self {
            consumer,
            assignment: WalAssignmentWatch::with_catch_up(metrics.clone(), gate),
            rebalance,
            metrics: metrics.clone(),
        }
    }
}

#[async_trait::async_trait]
impl WalConsumerPoll for BlockBuilderConsumer {
    fn take_revoked_partitions(&mut self) -> std::collections::BTreeSet<i32> {
        self.rebalance.take_revoked_partitions()
    }

    async fn poll(&mut self, window: Time) -> Result<Vec<ConsumerRecord>, TracesError> {
        let records = Consumer::poll(&mut self.consumer, window)
            .await
            .map_err(|err| TracesError::Wal(err.to_string()))?;
        // Read after the poll, so the snapshot is the one the fetch was served
        // against. An empty poll is observed too: a member that lost every
        // partition returns nothing and would otherwise look idle.
        self.assignment
            .observe_consumer(&self.consumer, !records.is_empty())
            .await;
        Ok(records)
    }
}

#[async_trait::async_trait]
impl WalConsumerCommit for BlockBuilderConsumer {
    async fn commit_sync(&mut self) -> Result<(), TracesError> {
        Consumer::commit_sync(&self.consumer)
            .await
            .map_err(|err| TracesError::Wal(err.to_string()))?;
        self.metrics.record_commit();
        self.assignment.observe_applied(&self.consumer).await;
        Ok(())
    }
}
