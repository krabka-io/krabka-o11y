//! What a WAL consumer group does to a block builder when its membership
//! changes, and the instrument that reports it.
//!
//! Every signal reads its WAL through a Kafka consumer group, and every role
//! that reads it buffers records across polls. The traces block builder merges
//! decoded windows until it holds `flush_max_records` records or the oldest one
//! reaches `flush_max_age`. The profiles block builder and the metrics
//! compactor do the same with their own thresholds. Each one commits offsets
//! only after it writes the block. So a flush threshold bounds the window
//! between a read and a durable write, and a poll does not.
//!
//! A group rebalance inside that window takes a partition away while this
//! member still holds decoded records for it. The listener below fences those
//! records out of the next flush so the new owner replays them.
//!
//! # Group membership and replay
//!
//! A group may scale up to its WAL partition count. A scale-out, rolling
//! restart, or session timeout moves partitions between members. The old owner
//! fences its uncommitted buffer and the new owner replays from the last durable
//! offset, so membership changes can add recovery work but cannot lose records.
//!
//! # Rebalance fencing
//!
//! [`WalRebalanceListener`] receives revocations inside `Consumer::poll`, before
//! the client releases the old assignment. Block builders then remove only the
//! revoked partitions from their in-memory buffers. Their offsets remain at
//! the last durable commit, so the new owner replays those records. Partitions
//! retained by this member keep both their buffered records and fetch position.
//!
//! # Assignment observability
//!
//! [`WalAssignmentWatch`] compares the assignment it saw last with the
//! assignment the consumer holds now. It moves
//! `wal_consumer_partition_owned` and `wal_consumer_partition_revocations`,
//! and it logs a warning that names each lost partition. A role calls
//! [`WalAssignmentWatch::observe`] once per poll.
//!
//! The counter is the alert. `last_consumed_offset` already moves
//! discontinuously when a partition leaves a member, and by itself that gauge
//! cannot say whether a consumer fell behind or lost the partition. The
//! revocation counter separates the two.

use std::collections::BTreeSet;

use crate::wal_consumer_metrics::WalConsumerMetrics;

#[cfg(test)]
mod tests;

mod wal_assignment_change;
mod wal_assignment_watch;
mod wal_rebalance_listener;

pub use self::{
    wal_assignment_change::WalAssignmentChange, wal_assignment_watch::WalAssignmentWatch,
    wal_rebalance_listener::WalRebalanceListener,
};
