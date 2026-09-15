use krabka_client_consumer::Consumer;

use super::{BTreeSet, WalAssignmentChange, WalConsumerMetrics};
use crate::ReadinessGate;

/// Reports every change to one consumer group member's partition assignment.
///
/// Build one beside the consumer, then call [`Self::observe`] once per poll
/// with what `Consumer::assignment` returned. The watch owns the comparison and
/// the instruments, so the four signals report a rebalance in the same shape.
///
/// The watch reports. It does not recover: see the
/// [module documentation](super) for why a block builder cannot flush a
/// partition it has already lost.
pub struct WalAssignmentWatch {
    metrics: WalConsumerMetrics,
    owned: BTreeSet<(String, i32)>,
    first_observation: bool,
    catch_up: Option<ReadinessGate>,
    unapplied_records: bool,
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

        for (topic, partition) in &change.gained {
            self.metrics.record_partition_assigned(topic, *partition);
        }
        for (topic, partition) in &change.revoked {
            self.metrics.record_partition_revoked(topic, *partition);
        }

        if change.strands_buffered_records() {
            tracing::error!(
                revoked = ?change.revoked,
                gained = ?change.gained,
                still_owned = self.owned.len(),
                "the consumer group took WAL partitions away from this member. \
                 Records polled from them and not yet written to a block are \
                 abandoned. Do not change a WAL consumer group's membership \
                 while it runs. The group id is not a scaling knob: set the WAL \
                 topic's partition count instead."
            );
        } else if !first_observation && !change.gained.is_empty() {
            // A pure gain means another member left the group or a new
            // partition appeared. Nothing of this member's is lost, but the
            // member that gave the partitions up did abandon its buffer for
            // them, and that member may be gone and unable to say so.
            tracing::warn!(
                gained = ?change.gained,
                still_owned = self.owned.len(),
                "the consumer group placed more WAL partitions on this member; \
                 another member left the group or lost them, and abandoned \
                 whatever it had buffered for them"
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
        self.unapplied_records |= has_records;
        self.record_recovery(&assigned, consumer.at_log_end().await);
        change
    }

    /// Marks every record observed since the last call as successfully
    /// applied, then refreshes recovery readiness from the consumer.
    pub async fn observe_applied(&mut self, consumer: &Consumer) {
        self.unapplied_records = false;
        let assigned = consumer.assignment().await;
        self.record_recovery(&assigned, consumer.at_log_end().await);
    }

    fn record_recovery(&self, assigned: &[(String, i32)], at_log_end: bool) {
        let caught_up = at_log_end && !self.unapplied_records;
        self.metrics.record_assignment(assigned, caught_up);
        if let Some(gate) = &self.catch_up {
            if caught_up {
                gate.mark_ready();
            } else {
                gate.mark_unready();
            }
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
}
