use super::BTreeSet;

/// The difference between two assignments of one consumer group member.
///
/// [`WalAssignmentWatch::observe`](super::WalAssignmentWatch::observe) returns
/// one per poll. Both lists are sorted by topic and then by partition.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WalAssignmentChange {
    /// Partitions the group took away from this member since the last poll.
    ///
    /// Each one requires any uncommitted buffer to be fenced from later writes.
    pub revoked: Vec<(String, i32)>,
    /// Partitions the group placed on this member since the last poll.
    pub gained: Vec<(String, i32)>,
}

impl WalAssignmentChange {
    pub(super) fn between(
        previous: &BTreeSet<(String, i32)>,
        current: &BTreeSet<(String, i32)>,
    ) -> Self {
        Self {
            revoked: previous.difference(current).cloned().collect(),
            gained: current.difference(previous).cloned().collect(),
        }
    }

    /// Whether the assignment is the same as the one seen last.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.revoked.is_empty() && self.gained.is_empty()
    }

    /// Whether the group took a partition away from this member.
    ///
    /// A gain on its own is safe because it brings no old buffer with it.
    #[must_use]
    pub fn strands_buffered_records(&self) -> bool {
        !self.revoked.is_empty()
    }
}
