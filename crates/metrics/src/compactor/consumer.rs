use std::collections::HashMap;

use krabka_client_consumer::OffsetAndMetadata;
use krabka_observability::{
    ReadinessGate,
    wal_consumer_metrics::WalConsumerMetrics,
    wal_group_assignment::{WalAssignmentWatch, WalRebalanceListener},
};

use super::{
    CompactionConsumerCommit, CompactionConsumerCommitError, CompactionConsumerPoll,
    CompactionConsumerPollError, Consumer, ConsumerError, ConsumerRecord, Time, async_trait,
};

/// The WAL consumer the metrics compactor drives.
///
/// It is the `krabka-client-consumer` [`Consumer`] and one
/// [`WalAssignmentWatch`] over it. The watch runs on every poll, so a rebalance
/// that takes partitions away from this member reaches the instruments and the
/// log instead of passing unseen.
///
/// The rebalance listener tells the compaction loop which partition buffers to
/// discard for replay before it merges records from the new assignment.
pub struct WalAssignmentConsumer {
    consumer: Consumer,
    assignment: tokio::sync::Mutex<WalAssignmentWatch>,
    rebalance: WalRebalanceListener,
    metrics: WalConsumerMetrics,
    drain: Option<WalDrain>,
    stopping: Option<krabka_observability::CancellationToken>,
}

struct WalDrain {
    config: super::MetricsCompactorConfig,
    security: Option<krabka_client_core::ClientSecurity>,
    frontier: Option<HashMap<(String, i32), i64>>,
    query: Option<Consumer>,
}

impl WalDrain {
    async fn capture(&mut self, consumer: &Consumer) -> Result<(), CompactionConsumerPollError> {
        if self.frontier.is_none() {
            // ReadCommitted.end_offsets is the LSO. A foreign transaction can
            // hide this distributor's acknowledged writes beyond it. Read the
            // high watermark through a query-only ReadUncommitted consumer.
            let query = Consumer::builder()
                .bootstrap(&self.config.bootstrap)
                .maybe_security(self.security.clone())
                .dispatch_queue_capacity(self.config.client_dispatch_queue_capacity.get())
                .frame_max(self.config.client_frame_max.size())
                .isolation_level(krabka_client_consumer::IsolationLevel::ReadUncommitted)
                .enable_auto_commit(false)
                .build()
                .await
                .map_err(CompactionConsumerPollError::Drain)?;
            let captured = async {
                let partitions = query
                    .partitions_for(&self.config.wal_topic)
                    .await
                    .map_err(CompactionConsumerPollError::Drain)?
                    .into_iter()
                    .map(|part| (part.topic, part.partition))
                    .collect::<Vec<_>>();
                if partitions.is_empty() {
                    return Err(CompactionConsumerPollError::Poll(
                        "cannot freeze an empty WAL partition catalog".into(),
                    ));
                }
                let end = query
                    .end_offsets(&partitions)
                    .await
                    .map_err(CompactionConsumerPollError::Drain)?;
                if end.len() != partitions.len() {
                    return Err(CompactionConsumerPollError::Poll(
                        "WAL drain boundary has missing partitions".into(),
                    ));
                }
                let begin = query
                    .beginning_offsets(&partitions)
                    .await
                    .map_err(CompactionConsumerPollError::Drain)?;
                let committed = consumer
                    .committed(&partitions)
                    .await
                    .map_err(CompactionConsumerPollError::Drain)?;
                for part in &partitions {
                    let applied = committed
                        .get(part)
                        .and_then(Option::as_ref)
                        .map_or(0, |offset| offset.offset);
                    if begin.get(part).is_none_or(|begin| *begin > applied) {
                        return Err(CompactionConsumerPollError::Poll(format!(
                            "WAL retention passed the durable drain frontier for {part:?}"
                        )));
                    }
                }
                Ok(end)
            }
            .await;
            match captured {
                Ok(frontier) => {
                    self.frontier = Some(frontier);
                    self.query = Some(query);
                }
                Err(error) => {
                    let _ = query.close().await;
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    async fn check_retention(
        &self,
        consumer: &Consumer,
    ) -> Result<(), CompactionConsumerPollError> {
        let frontier = self.frontier.as_ref().expect("drain boundary was captured");
        let partitions = frontier.keys().cloned().collect::<Vec<_>>();
        let begin = self
            .query
            .as_ref()
            .expect("drain query remains open")
            .beginning_offsets(&partitions)
            .await
            .map_err(CompactionConsumerPollError::Drain)?;
        let committed = consumer
            .committed(&partitions)
            .await
            .map_err(CompactionConsumerPollError::Drain)?;
        for part in partitions {
            let applied = committed
                .get(&part)
                .and_then(Option::as_ref)
                .map_or(0, |offset| offset.offset);
            if begin.get(&part).is_none_or(|begin| *begin > applied) {
                return Err(CompactionConsumerPollError::Poll(format!(
                    "WAL retention passed the durable drain frontier for {part:?}"
                )));
            }
        }
        Ok(())
    }
}

impl WalAssignmentConsumer {
    /// Wraps `consumer` and reports its assignment through `metrics`.
    #[must_use]
    pub fn new(
        consumer: Consumer,
        metrics: &WalConsumerMetrics,
        rebalance: WalRebalanceListener,
    ) -> Self {
        Self {
            consumer,
            assignment: tokio::sync::Mutex::new(WalAssignmentWatch::new(metrics.clone())),
            rebalance,
            metrics: metrics.clone(),
            drain: None,
            stopping: None,
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
            assignment: tokio::sync::Mutex::new(WalAssignmentWatch::with_catch_up(
                metrics.clone(),
                gate,
            )),
            rebalance,
            metrics: metrics.clone(),
            drain: None,
            stopping: None,
        }
    }

    pub(crate) fn with_drain(
        mut self,
        config: &super::MetricsCompactorConfig,
        security: Option<krabka_client_core::ClientSecurity>,
    ) -> Self {
        self.drain = Some(WalDrain {
            config: config.clone(),
            security,
            frontier: None,
            query: None,
        });
        self
    }

    pub(crate) fn drain_on_shutdown(&mut self, stopping: krabka_observability::CancellationToken) {
        self.stopping = Some(stopping);
    }

    /// Closes the WAL consumer and awaits its coordinator's shutdown.
    ///
    /// # Errors
    /// Returns any error from the consumer's rebalance listener on close.
    pub async fn close(self) -> Result<(), ConsumerError> {
        let query = match self.drain.and_then(|drain| drain.query) {
            Some(query) => query.close().await,
            None => Ok(()),
        };
        let consumer = self.consumer.close().await;
        query.and(consumer)
    }
}

#[async_trait]
impl CompactionConsumerPoll for WalAssignmentConsumer {
    fn take_revoked_partitions(&mut self) -> std::collections::BTreeSet<i32> {
        self.rebalance.take_revoked_partitions()
    }

    async fn drain_complete(&mut self) -> Result<bool, CompactionConsumerPollError> {
        let Some(drain) = &mut self.drain else {
            return Ok(true);
        };
        drain.capture(&self.consumer).await?;
        let frontier = drain
            .frontier
            .as_ref()
            .expect("drain boundary was captured");
        let partitions = frontier.keys().cloned().collect::<Vec<_>>();
        let committed = self
            .consumer
            .committed(&partitions)
            .await
            .map_err(CompactionConsumerPollError::Drain)?;
        // The loop has persisted every delivered record. Advance through
        // skipped transaction/control records using the consumer's actual
        // positions, never the target end offsets.
        let mut applied = HashMap::new();
        for (topic, partition) in self.consumer.assignment().await {
            if topic == drain.config.wal_topic {
                let position = self
                    .consumer
                    .position(&topic, partition)
                    .await
                    .map_err(CompactionConsumerPollError::Drain)?;
                if committed
                    .get(&(topic.clone(), partition))
                    .and_then(Option::as_ref)
                    .is_none_or(|offset| offset.offset < position)
                {
                    applied.insert((topic, partition), OffsetAndMetadata::new(position));
                }
            }
        }
        if !applied.is_empty() {
            self.consumer
                .commit_offsets_sync(applied)
                .await
                .map_err(CompactionConsumerPollError::Drain)?;
            self.metrics.record_commit();
        }
        let committed = self
            .consumer
            .committed(&partitions)
            .await
            .map_err(CompactionConsumerPollError::Drain)?;
        // All partitions count, including ones another group member owns.
        // Later producers cannot move this boundary. No empty poll or readiness
        // flag can stand in for durable block/index writes and their commits.
        Ok(frontier.iter().all(|(part, end)| {
            *end == 0
                || committed
                    .get(part)
                    .and_then(Option::as_ref)
                    .is_some_and(|offset| offset.offset >= *end)
        }))
    }

    async fn poll(
        &mut self,
        timeout: Time,
    ) -> Result<Vec<ConsumerRecord>, CompactionConsumerPollError> {
        if let Some(drain) = &mut self.drain {
            if self
                .stopping
                .as_ref()
                .is_some_and(krabka_observability::CancellationToken::is_cancelled)
            {
                // Freeze before a shutdown poll can reset past expired records.
                drain.capture(&self.consumer).await?;
            }
            if drain.frontier.is_some() {
                drain.check_retention(&self.consumer).await?;
            }
        }
        let records = Consumer::poll(&mut self.consumer, timeout)
            .await
            .map_err(|error| CompactionConsumerPollError::Poll(error.to_string()))?;
        // Read after the poll, so the snapshot is the one the fetch was served
        // against. An empty poll is observed too: a member that lost every
        // partition returns nothing and would otherwise look idle.
        self.assignment
            .lock()
            .await
            .observe_consumer(&self.consumer, !records.is_empty())
            .await;
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
            .map(|offset| {
                (
                    (topic.to_string(), offset.partition.0),
                    OffsetAndMetadata::new(offset.offset.0),
                )
            })
            .collect();
        Consumer::commit_offsets_sync(&self.consumer, offsets)
            .await
            .map_err(|error| CompactionConsumerCommitError::Commit(error.to_string()))?;
        self.metrics.record_commit();
        self.assignment
            .lock()
            .await
            .observe_applied(&self.consumer)
            .await;
        Ok(())
    }
}
