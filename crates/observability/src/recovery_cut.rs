//! The broker half of a deployment backup.
//!
//! `krabka-blockstore` copies and verifies the object-store and local-state
//! parts of a deployment cut, and it reads the broker through the
//! [`BrokerState`](krabka_blockstore::BrokerState) seam. [`KafkaBrokerState`]
//! is that seam over the Kafka admin protocol: it reads the next offset of
//! every partition in the cut and every committed consumer-group offset on
//! those partitions.
//!
//! [`deployment_drained_groups`] names the consumer groups that write blocks.
//! A backup refuses a cut where one of them has not committed the end of its
//! topic, because a restored block builder would write those records again
//! under other block keys.

mod deployment_drained_groups;
mod kafka_broker_state;

pub use self::{
    deployment_drained_groups::deployment_drained_groups, kafka_broker_state::KafkaBrokerState,
};
