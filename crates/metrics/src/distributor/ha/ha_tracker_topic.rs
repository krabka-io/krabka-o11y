/// The compacted HA-tracker topic: `(tenant, cluster)` to the elected replica.
///
/// Its `cleanup.policy=compact` requirement is part of the topic contract in
/// [`krabka_observability::topic_contract`], which this crate re-exports the
/// name from.
pub use krabka_observability::topic_contract::METRICS_HA_TOPIC as HA_TRACKER_TOPIC;
