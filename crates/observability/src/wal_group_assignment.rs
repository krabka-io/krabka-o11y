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
//! A group rebalance inside that window abandons the buffer. The group takes a
//! partition away from this member, and the member still holds the decoded
//! records for it. Nothing writes them.
//!
//! # The group id is not a scaling knob
//!
//! A group with a fixed membership is safe. Each member owns its own
//! partitions, buffers only those partitions, and commits only what it wrote.
//! Two members that never join or leave lose nothing.
//!
//! Every membership change is the problem, and the change does not need an
//! operator to cause one. A scale-out adds a member. A rolling restart removes
//! one and adds it back. A poll that overruns the session timeout evicts a
//! member that is still running. Each of these moves partitions, and each
//! partition that moves carries an abandoned buffer with it.
//!
//! So `--compactor-group-id`, `--block-builder-group-id`, `--wal-group-id` and
//! their siblings name the group a role joins. They do not make the write path
//! scale. Set the WAL topic's partition count for that, and keep the group's
//! membership fixed while it runs. The partition count is the shard count, and
//! [`topic_contract`](crate::topic_contract) states it once.
//!
//! # Why this repository cannot fix it
//!
//! A Kafka client fixes this with a rebalance listener. The client calls the
//! application back before it releases the assignment, the application flushes
//! the partitions it is about to lose, and the commit that follows lands under
//! the old generation. librdkafka spells this `rebalance_cb`, the Java client
//! spells it `ConsumerRebalanceListener.onPartitionsRevoked`, and Loki and
//! Mimir both flush their Kafka ingesters from it.
//!
//! The pinned `krabka-client-consumer` exposes no such callback. Its whole
//! public consumer surface is `poll`, `seek`, `commit_sync`,
//! `commit_offsets_sync`, `commit_async`, `assignment`, `generation_id`,
//! `group_metadata`, `member_id`, `subscribed_topics`, `group_id` and `close`.
//! Its own `poll` documentation states the design: "The internal coordinator
//! task handles rebalances transparently and mutates the live `assigned`
//! snapshot in place." A background task changes the assignment, and no
//! application code runs between the decision and the change. The crate is
//! resolved through `[patch.crates-io]` from a sibling repository, so this
//! repository cannot add the callback either.
//!
//! A flush that runs after the assignment already moved is not a fix. It writes
//! a block for a partition another member now reads, under an object key that
//! is a function of this member's own buffered offset range. The two members
//! pick different flush boundaries, so the keys differ, and the result is two
//! overlapping blocks with the same rows in both. That is why this module
//! reports the event and does not try to recover from it.
//!
//! # What the sibling repository would have to add
//!
//! One callback, and it has to run before the client publishes the new
//! assignment: give the application the partitions the group is about to take,
//! let it return a result, and hold the round until it does. With that, a block
//! builder flushes the buffer for those partitions and commits their offsets
//! under the old generation, and a flush that fails stops the member instead of
//! losing its records. Nothing smaller works, because every smaller shape tells
//! the application after the fact.
//!
//! Until then, static group membership is the one mitigation this repository
//! can reach. `Consumer::builder().group_instance_id(..)` sets the KIP-345
//! instance id. The broker keeps a static member's slot past the session
//! timeout and hands the same assignment back on rejoin, so a restart moves no
//! partition and the rest of the group does not rebalance. It does nothing for
//! a scale-out, for a scale-in, or for a member that leaves for good.
//!
//! # What this module does instead
//!
//! [`WalAssignmentWatch`] compares the assignment it saw last with the
//! assignment the consumer holds now. It moves
//! `wal_consumer_partition_owned` and `wal_consumer_partition_revocations`,
//! and it logs an error that names each lost partition. A role calls
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

pub use self::{
    wal_assignment_change::WalAssignmentChange, wal_assignment_watch::WalAssignmentWatch,
};
