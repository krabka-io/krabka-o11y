//! The Kafka topic contract: what the six topics must be, how they are
//! created, and what a role does when a live topic differs.
//!
//! Every signal in this stack writes a Kafka topic and reads it back. The
//! topics carry requirements that nothing on the wire states:
//!
//! - A WAL topic's **partition count is the write-path shard count**. The
//!   producer routes a keyed record with `murmur2(key) % partition_count`, so
//!   the count decides which shard a series, a trace or a stream lives on.
//!   Change it and every key re-maps: a consumer still reads records in
//!   offset order per partition, but one series' history is now split across
//!   two partitions with no order between them. Nothing reports this.
//! - A state topic's **`cleanup.policy` must be `compact`**. The HA tracker
//!   and the ruler keep their state as the last record per key. Under the
//!   Kafka default of `delete` the broker drops the whole map at the
//!   retention window, so an election is re-run and every pending alert
//!   re-fires after a restart.
//! - A WAL topic's **`retention.ms` is backpressure, not housekeeping**. It
//!   is the only limit on how far a stalled block-builder may fall behind
//!   before the broker drops records that were never written to a block.
//!
//! # Create, then check
//!
//! [`ensure_topics`] creates what is missing and then describes every topic
//! and compares it. The two steps are separate because creating proves
//! nothing: `CreateTopics` against an existing topic returns
//! `TOPIC_ALREADY_EXISTS` and leaves the topic exactly as it was, wrong
//! partition count included. [`verify_topics`] is the same check without the
//! create.
//!
//! # One definition of the shard count
//!
//! The partition count is stated once, by the deployment step that provisions
//! the topics, and read back everywhere else. [`verify_topics`] compares it
//! because the caller is that step and knows what it asked for.
//! [`check_topics`] does not: a role checks that its topics exist and that a
//! state topic is compacted, then reads the live shard count out of
//! [`TopicReport::partitions`]. A role that carried its own copy of the number
//! would be the second, independently configured shard count that this
//! contract removes.
//!
//! # What a difference costs
//!
//! [`TopicDrift`] separates the differences that corrupt data from the
//! differences an operator can see and fix while the stack runs.
//! [`TopicDrift::is_fatal`] holds the split, and only fatal drift stops a
//! role from starting.
//!
//! # Who runs it
//!
//! `krabka-o11y-bootstrap` provisions [`ALL_TOPICS`] with
//! [`provision_topics`], as a separate deployment step. Every role calls
//! [`require_topics`] at startup for the topics that role touches -- see
//! [`METRICS_TOPICS`] and its siblings -- and refuses to start when one is
//! absent or not compacted. Concurrent provisioning is expected and safe: one
//! `CreateTopics` wins and the rest read `TOPIC_ALREADY_EXISTS`, which is
//! treated as success and then validated like any other pre-existing topic.
//!
//! # What the broker does not tell us
//!
//! `DescribeConfigs` on this broker returns per-topic **overrides** and
//! nothing else. A key nobody set is absent from the response rather than
//! reported at its effective value, so an absent `cleanup.policy` cannot be
//! told apart from a `cleanup.policy` that happens to equal the broker
//! default. The contract therefore sets both keys explicitly on create, and
//! treats an absent `cleanup.policy` on a state topic as a fatal difference:
//! the Kafka default is `delete`, and assuming otherwise is the silent loss
//! this module exists to stop.

#[cfg(test)]
mod tests;

mod config_keys;
mod desired_specs;
mod ensure_topics;
mod inspect_topics;
mod observed_topic;
mod partition_count;
mod provision_topics;
mod require_topics;
mod shard_count;
mod topic_contract_entry;
mod topic_contract_error;
mod topic_drift;
mod topic_expectation;
mod topic_groups;
mod topic_kind;
mod topic_names;
mod topic_report;
mod topic_settings;
mod verify_topics;

pub use config_keys::{CLEANUP_POLICY, COMPACT, RETENTION_MS};
pub use desired_specs::desired_specs;
pub use ensure_topics::ensure_topics;
pub(crate) use inspect_topics::inspect_topics;
pub use observed_topic::ObservedTopic;
pub use partition_count::PartitionCount;
pub use provision_topics::provision_topics;
pub use require_topics::require_topics;
pub use shard_count::shard_count;
pub use topic_contract_entry::TopicContract;
pub use topic_contract_error::TopicContractError;
pub use topic_drift::TopicDrift;
pub(crate) use topic_expectation::TopicExpectation;
pub use topic_groups::{
    ALL_TOPICS, LOGS_TOPICS, METRICS_SERVICE_TOPICS, METRICS_TOPICS, PROFILES_TOPICS, TRACES_TOPICS,
};
pub use topic_kind::TopicKind;
pub use topic_names::{
    LOGS_WAL_TOPIC, METRICS_HA_TOPIC, METRICS_RULER_STATE_TOPIC, METRICS_WAL_TOPIC,
    PROFILES_WAL_TOPIC, TRACES_WAL_TOPIC,
};
pub use topic_report::TopicReport;
pub use topic_settings::TopicSettings;
pub use verify_topics::{check_topics, verify_topics};
