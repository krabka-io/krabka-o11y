//! The broker half of a deployment backup.
//!
//! `krabka-blockstore` copies and verifies the object-store and local-state
//! parts of a deployment cut, and it reads the broker through the
//! [`BrokerState`](krabka_blockstore::BrokerState) seam. [`KafkaBrokerState`]
//! is that seam over the Kafka admin protocol: it reads the next offset of
//! every partition in the cut and every committed consumer-group offset on
//! those partitions.
//!
//! [`DeploymentKafkaNames`] names the topics of a cut and the consumer groups
//! that write blocks, and [`deployment_drained_groups`] gives the default
//! groups. A backup refuses a cut where one of those groups has not committed
//! the end of its topic, because a restored block builder would write those
//! records again under other block keys.

mod deployment_drained_groups;
mod deployment_kafka_names;
mod kafka_broker_state;

pub use self::{
    deployment_drained_groups::deployment_drained_groups,
    deployment_kafka_names::DeploymentKafkaNames, kafka_broker_state::KafkaBrokerState,
};
