/// The traces WAL topic name.
///
/// The name, the partition count and the retention window are one contract,
/// held in [`krabka_observability::topic_contract`]. This crate re-exports the
/// name so that the distributor, the block-builder and the provisioning step
/// cannot drift apart.
pub use krabka_observability::topic_contract::TRACES_WAL_TOPIC;
