use std::collections::BTreeMap;

use krabka_client_admin::CreateTopicSpec;
use krabka_units::convert::TimeExt as _;

use super::{CLEANUP_POLICY, COMPACT, RETENTION_MS, TopicContract, TopicKind, TopicSettings};

/// Turns the contract and the deployment settings into `CreateTopics` specs.
///
/// A WAL topic gets the WAL partition count and an explicit `retention.ms`. A
/// compacted state topic gets the state partition count and an explicit
/// `cleanup.policy=compact`, and no `retention.ms`: under compaction that key
/// bounds how long a superseded record survives, which is not what the
/// contract wants to state.
///
/// Both keys are set explicitly even where the value matches a broker
/// default. The broker reports per-topic overrides and nothing else, so a key
/// that is left at its default is a key no later start can check.
#[must_use]
pub fn desired_specs(topics: &[TopicContract], settings: &TopicSettings) -> Vec<CreateTopicSpec> {
    topics
        .iter()
        .map(|topic| {
            let (partitions, configs) = match topic.kind {
                TopicKind::Wal => (
                    settings.wal_partitions,
                    BTreeMap::from([(
                        RETENTION_MS.to_string(),
                        settings.wal_retention.millis_i64().to_string(),
                    )]),
                ),
                TopicKind::CompactedState => (
                    settings.state_partitions,
                    BTreeMap::from([(CLEANUP_POLICY.to_string(), COMPACT.to_string())]),
                ),
            };
            CreateTopicSpec {
                name: topic.name.to_string(),
                partitions: partitions.get(),
                replicas: settings.replication_factor,
                configs,
            }
        })
        .collect()
}
