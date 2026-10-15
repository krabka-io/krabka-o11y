use krabka_observability::{
    ReadinessGate,
    persisted_format::{PersistedHeader, validate_persisted_format},
    wal_consumer_metrics::WalConsumerMetrics,
    wal_group_assignment::{WalAssignmentWatch, WalRebalanceListener},
};

use super::*;

/// Minimal WAL-consumer commit surface the block-builder loop drives.
///
/// This trait stays separate from [`crate::blockbuilder::WalConsumerPoll`], so a test can express
/// the commit-only invariant as its own recorded call. That invariant is that a
/// commit happens strictly after a durable flush.
#[async_trait::async_trait]
pub trait WalConsumerCommit: Send {
    async fn commit_sync(&mut self) -> Result<(), TracesError>;
}

/// Minimal WAL-consumer poll surface the block-builder loop drives.
///
/// `run` takes this trait rather than the concrete
/// [`krabka_client_consumer::Consumer`], so a scripted fake can drive the
/// offset-commit invariants in tests. The record type matches what
/// [`decode_consumer_records`] consumes, so the loop body stays the same.
#[async_trait::async_trait]
pub trait WalConsumerPoll: Send {
    fn take_revoked_partitions(&mut self) -> BTreeSet<i32> {
        BTreeSet::new()
    }

    async fn poll(&mut self, window: Time) -> Result<Vec<ConsumerRecord>, TracesError>;
}

/// Decode Kafka consumer records into per-partition span windows.
///
/// This function ignores tombstones and records without values. They do not
/// extend the inclusive offset range.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn decode_consumer_records(
    records: &[ConsumerRecord],
) -> Result<BTreeMap<i32, PartitionWindow>, TracesError> {
    let mut windows = BTreeMap::<i32, PartitionWindow>::new();
    for record in records {
        validate_persisted_format(record.headers.iter().map(|header| PersistedHeader {
            key: header.key.as_str(),
            value: header.value.as_deref(),
        }))
        .map_err(|error| TracesError::Wal(error.to_string()))?;
        let Some(value) = &record.value else {
            continue;
        };
        let decoded = SpanRecord::decode(value)?;
        let window = windows.entry(record.partition).or_insert(PartitionWindow {
            offset_range: OffsetRange::new(record.offset, record.offset),
            records: Vec::new(),
        });
        window.offset_range.min = window.offset_range.min.min(record.offset);
        window.offset_range.max = window.offset_range.max.max(record.offset);
        window.records.push(decoded);
    }
    Ok(windows)
}

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
