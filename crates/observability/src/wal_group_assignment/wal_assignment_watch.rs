use krabka_client_consumer::Consumer;

use super::{BTreeSet, WalAssignmentChange, WalConsumerMetrics};

/// Reports every change to one consumer group member's partition assignment.
///
/// Build one beside the consumer, then call [`Self::observe`] once per poll
/// with what `Consumer::assignment` returned. The watch owns the comparison and
/// the instruments, so the four signals report a rebalance in the same shape.
///
/// The watch reports. It does not recover: see the
/// [module documentation](super) for why a block builder cannot flush a
/// partition it has already lost.
#[derive(Debug)]
pub struct WalAssignmentWatch {
    metrics: WalConsumerMetrics,
    owned: BTreeSet<(String, i32)>,
    first_observation: bool,
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
    /// the state the watch most needs to see.
    pub async fn observe_consumer(&mut self, consumer: &Consumer) -> WalAssignmentChange {
        self.observe(&consumer.assignment().await)
    }

    /// The partitions this member holds, as of the last [`Self::observe`] call.
    #[must_use]
    pub fn owned(&self) -> &BTreeSet<(String, i32)> {
        &self.owned
    }
}
