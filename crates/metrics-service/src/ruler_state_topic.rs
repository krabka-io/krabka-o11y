/// The compacted ruler-state topic: alert and rule-group state per entity.
///
/// Its `cleanup.policy=compact` requirement is part of the topic contract in
/// [`krabka_observability::topic_contract`], which this crate re-exports the
/// name from.
pub use krabka_observability::topic_contract::METRICS_RULER_STATE_TOPIC as RULER_STATE_TOPIC;
