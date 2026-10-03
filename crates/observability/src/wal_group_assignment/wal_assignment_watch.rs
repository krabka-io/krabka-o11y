use krabka_client_consumer::Consumer;
use krabka_units::prelude::{Time, TimeExt as _, secs};

use super::{BTreeSet, WalAssignmentChange, WalConsumerMetrics};
use crate::ReadinessGate;

/// The longest a poll loop waits for the broker to confirm the durable log
/// end.
const LOG_END_QUERY_TIMEOUT: Time = secs(5);

/// Whether the group's committed offset of every partition in `assigned` has
/// reached the end offset that the broker reports now. With `read_committed`,
/// that end offset is the last stable offset.
async fn broker_confirms_durable_log_end(consumer: &Consumer, assigned: &[(String, i32)]) -> bool {
    let Ok(ends) = consumer.end_offsets(assigned).await else {
        return false;
    };
    let Ok(committed) = consumer.committed(assigned).await else {
        return false;
    };
    assigned.iter().all(|partition| {
        let end = ends.get(partition);
        let committed = committed.get(partition).and_then(Option::as_ref);
        match (end, committed) {
            (Some(end), Some(committed)) => committed.offset >= *end,
            // A partition that never held a record has nothing to replay.
            (Some(0), None) => true,
            _ => false,
        }
    })
}

/// Reports every change to one consumer group member's partition assignment.
///
/// Build one beside the consumer, then call [`Self::observe`] once per poll
/// with what `Consumer::assignment` returned. The watch owns the comparison and
/// the instruments, so the four signals report a rebalance in the same shape.
///
/// The watch reports assignment changes; [`crate::wal_group_assignment::WalRebalanceListener`] supplies the
/// synchronous fencing signal used to recover them.
pub struct WalAssignmentWatch {
    metrics: WalConsumerMetrics,
    owned: BTreeSet<(String, i32)>,
    first_observation: bool,
    catch_up: Option<ReadinessGate>,
    unapplied_records: bool,
    caught_up_after_apply: bool,
}

impl WalAssignmentWatch {
    /// Builds a watch that has seen no assignment yet.
    ///
    /// The first [`Self::observe`] call reports every assigned partition as a
    /// gain and none as a revocation, so a role that starts up does not log an
    /// error for partitions it never held.
    #[must_use]
    pub fn new(metrics: WalConsumerMetrics) -> Self {
        Self {
            metrics,
            owned: BTreeSet::new(),
            first_observation: true,
            catch_up: None,
            unapplied_records: false,
            caught_up_after_apply: false,
        }
    }

    /// Builds a watch that also keeps a recovery readiness gate in step with
    /// the consumer's broker-observed end offsets.
    #[must_use]
    pub fn with_catch_up(metrics: WalConsumerMetrics, catch_up: ReadinessGate) -> Self {
        Self {
            catch_up: Some(catch_up),
            ..Self::new(metrics)
        }
    }

    /// Compares `assigned` with the assignment seen last and reports the
    /// difference.
    ///
    /// The call sets `wal_consumer_partition_owned` for each partition this
    /// member gained and clears it for each partition the group took away. It
    /// also increments `wal_consumer_partition_revocations` once per lost
    /// partition, and logs one error that names them all.
    ///
    /// An empty `assigned` is a real state and the watch treats it as one: a
    /// member that holds nothing has lost everything it held.
    pub fn observe(&mut self, assigned: &[(String, i32)]) -> WalAssignmentChange {
        let current: BTreeSet<(String, i32)> = assigned.iter().cloned().collect();
        let change = WalAssignmentChange::between(&self.owned, &current);
        self.owned = current;
        let first_observation = std::mem::replace(&mut self.first_observation, false);

        if !change.gained.is_empty() || !change.revoked.is_empty() {
            self.caught_up_after_apply = false;
            if let Some(gate) = &self.catch_up {
                gate.mark_unready();
            }
        }

        for (topic, partition) in &change.gained {
            self.metrics.record_partition_assigned(topic, *partition);
        }
        for (topic, partition) in &change.revoked {
            self.metrics.record_partition_revoked(topic, *partition);
        }

        if change.strands_buffered_records() {
            tracing::warn!(
                revoked = ?change.revoked,
                gained = ?change.gained,
                still_owned = self.owned.len(),
                "the consumer group took WAL partitions away from this member; \
                 buffered records for them were fenced and will replay from \
                 the last durable offset"
            );
        } else if !first_observation && !change.gained.is_empty() {
            // A pure gain means another member left the group or a new
            // partition appeared. Nothing of this member's is lost, but the
            // member that gave the partitions up fenced its local buffer, and
            // that member may be gone and unable to report the revocation.
            tracing::warn!(
                gained = ?change.gained,
                still_owned = self.owned.len(),
                "the consumer group placed more WAL partitions on this member; \
                 another member left the group or lost them; replay begins at \
                 the last durable offset"
            );
        }

        change
    }

    /// Reads `consumer`'s live assignment and reports the change, as
    /// [`Self::observe`] does.
    ///
    /// This is the call a poll loop makes. Call it on every poll, including an
    /// empty one: a member that lost every partition polls nothing, and that is
    /// the state the watch most needs to see. Pass `has_records` for the batch
    /// just fetched, then call [`Self::observe_applied`] only after those
    /// records have been decoded and durably applied.
    pub async fn observe_consumer(
        &mut self,
        consumer: &Consumer,
        has_records: bool,
    ) -> WalAssignmentChange {
        let assigned = consumer.assignment().await;
        let change = self.observe(&assigned);
        let mut at_log_end = consumer.at_log_end().await;
        self.caught_up_after_apply |= at_log_end && (has_records || self.unapplied_records);
        self.unapplied_records |= has_records;
        if !has_records && !at_log_end && self.durable_log_end(consumer, &assigned).await {
            // Every record up to the log end is durable, so nothing this
            // member counted as unapplied is still owed. A rebalance may have
            // fenced those records, and then no apply ever reports them.
            self.unapplied_records = false;
            at_log_end = true;
        }
        self.record_recovery(&assigned, at_log_end);
        change
    }

    /// Marks every record observed since the last call as successfully
    /// applied, then refreshes recovery readiness from the consumer.
    pub async fn observe_applied(&mut self, consumer: &Consumer) {
        let assigned = consumer.assignment().await;
        self.record_applied(&assigned, consumer.at_log_end().await);
    }

    /// Whether the broker confirms that the group has durably committed every
    /// assigned partition through its log end.
    ///
    /// `Consumer::at_log_end` compares positions with the end offsets that
    /// fetch responses carry. The client clears those end offsets when the
    /// group publishes a new assignment, and an incremental fetch session
    /// omits a partition that has no new records and an unchanged high
    /// watermark. A partition that this member keeps through a rebalance can
    /// therefore stay without an end offset until a producer writes to it.
    /// The rebalance can also fence records that this member counted as
    /// unapplied, and then no apply reports them. On an idle WAL, either case
    /// keeps the catch-up gate closed until a producer writes again.
    ///
    /// This asks the broker only while the gate is closed. The query is
    /// bounded by [`LOG_END_QUERY_TIMEOUT`], and a failed or late answer
    /// counts as "not at the end".
    async fn durable_log_end(&self, consumer: &Consumer, assigned: &[(String, i32)]) -> bool {
        let gate_closed = self.catch_up.as_ref().is_some_and(|gate| !gate.is_ready());
        if !gate_closed || assigned.is_empty() {
            return false;
        }
        tokio::time::timeout(
            LOG_END_QUERY_TIMEOUT.to_std(),
            broker_confirms_durable_log_end(consumer, assigned),
        )
        .await
        .unwrap_or(false)
    }

    fn record_applied(&mut self, assigned: &[(String, i32)], at_log_end: bool) {
        self.unapplied_records = false;
        let caught_up_after_apply = std::mem::take(&mut self.caught_up_after_apply);
        self.record_recovery(assigned, at_log_end);
        if caught_up_after_apply && let Some(gate) = &self.catch_up {
            gate.mark_ready();
        }
    }

    fn record_recovery(&self, assigned: &[(String, i32)], at_log_end: bool) {
        let caught_up = at_log_end && !self.unapplied_records;
        self.metrics.record_assignment(assigned, caught_up);
        if let Some(gate) = &self.catch_up
            && caught_up
        {
            gate.mark_ready();
        }
    }

    /// The partitions this member holds, as of the last [`Self::observe`] call.
    #[must_use]
    pub fn owned(&self) -> &BTreeSet<(String, i32)> {
        &self.owned
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;

    #[test]
    fn log_end_is_not_ready_until_the_fetched_batch_is_applied() {
        let metrics = WalConsumerMetrics::unregistered();
        let readiness = crate::RoleReadiness::new();
        let gate = readiness.gate("wal-catch-up");
        let mut watch = WalAssignmentWatch::with_catch_up(metrics.clone(), gate.clone());
        let assigned = [("wal".to_string(), 0)];

        watch.unapplied_records = true;
        watch.record_recovery(&assigned, true);
        assert2::check!(!gate.is_ready());
        assert2::check!(!metrics.recovery_status().caught_up);

        watch.unapplied_records = false;
        watch.record_recovery(&assigned, true);
        assert2::check!(gate.is_ready());
        assert2::check!(metrics.recovery_status().caught_up);
    }

    #[test]
    fn applied_log_end_snapshot_stays_ready_when_the_broker_advances() {
        let metrics = WalConsumerMetrics::unregistered();
        let readiness = crate::RoleReadiness::new();
        let gate = readiness.gate("wal-catch-up");
        let mut watch = WalAssignmentWatch::with_catch_up(metrics.clone(), gate.clone());
        let assigned = [("wal".to_string(), 0)];

        watch.unapplied_records = true;
        watch.caught_up_after_apply = true;
        watch.record_applied(&assigned, false);

        assert2::check!(gate.is_ready());
        assert2::check!(!metrics.recovery_status().caught_up);

        watch.observe(&[("wal".to_string(), 1)]);
        assert2::check!(!gate.is_ready());
    }
}
