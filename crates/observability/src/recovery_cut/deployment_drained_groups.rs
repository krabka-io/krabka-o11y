use krabka_blockstore::DrainedGroup;

use crate::topic_contract::{
    LOGS_WAL_TOPIC, METRICS_WAL_TOPIC, PROFILES_WAL_TOPIC, TRACES_WAL_TOPIC,
};

/// The default block-builder consumer group of each signal, with the WAL
/// topic it reads.
///
/// These are the groups that write blocks. Each one must have committed the
/// end of its WAL before a deployment cut is sealed. A deployment that sets
/// another `wal-group-id` names its own groups.
#[must_use]
pub fn deployment_drained_groups() -> Vec<DrainedGroup> {
    [
        ("krabka-metrics-block-builder", METRICS_WAL_TOPIC),
        ("krabka-observability-block-builder", LOGS_WAL_TOPIC),
        ("krabka-profiles-block-builder", PROFILES_WAL_TOPIC),
        ("krabka-traces-block-builder", TRACES_WAL_TOPIC),
    ]
    .into_iter()
    .map(|(group, topic)| DrainedGroup {
        group: group.into(),
        topic: topic.into(),
    })
    .collect()
}
