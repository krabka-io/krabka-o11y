use krabka_client_admin::{TopicConfigOverrides, TopicMetadata};

use super::{
    CLEANUP_POLICY, COMPACT, ObservedTopic, PartitionCount, RETENTION_MS, TopicContract,
    TopicDrift, TopicExpectation, TopicKind,
};

/// Kafka's `UNKNOWN_TOPIC_OR_PARTITION`. A broker answers `Metadata` for a
/// topic it does not hold with this code rather than by leaving the topic out
/// of the response, so it reads as "absent" and not as "unreadable".
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

/// Compares what the broker reports against what the contract requires.
///
/// This is the describe-and-compare half of provisioning, and it is a pure
/// function so that every branch is reachable without a broker. It runs
/// whether or not the caller created anything: a `CreateTopics` on a topic
/// that already exists succeeds as a no-op and leaves the wrong configuration
/// in place, so creating proves nothing on its own.
pub(crate) fn inspect_topics(
    contracts: &[TopicContract],
    expectation: &TopicExpectation<'_>,
    metadata: &TopicMetadata,
    configs: &[TopicConfigOverrides],
) -> (Vec<ObservedTopic>, Vec<TopicDrift>) {
    let mut observed = Vec::with_capacity(contracts.len());
    let mut drift = Vec::new();

    for (index, contract) in contracts.iter().enumerate() {
        let spec = match expectation {
            TopicExpectation::Deployment(specs) => specs.get(index),
            TopicExpectation::Structure => None,
        };
        let Some(entry) = metadata.topics.iter().find(|t| t.name == contract.name) else {
            drift.push(TopicDrift::Missing {
                topic: contract.name.to_string(),
                purpose: contract.purpose,
            });
            continue;
        };
        if let Some(error) = &entry.error {
            drift.push(if error.code == UNKNOWN_TOPIC_OR_PARTITION {
                TopicDrift::Missing {
                    topic: contract.name.to_string(),
                    purpose: contract.purpose,
                }
            } else {
                TopicDrift::Unreadable {
                    topic: contract.name.to_string(),
                    error: error.name.to_string(),
                }
            });
            continue;
        }
        // A topic with no partitions is a topic the broker does not hold.
        // `Metadata` reports it that way when the topic was deleted between
        // the create and the describe.
        let Ok(partitions) = PartitionCount::new(entry.partition_count) else {
            drift.push(TopicDrift::Missing {
                topic: contract.name.to_string(),
                purpose: contract.purpose,
            });
            continue;
        };

        let overrides = configs
            .iter()
            .find(|c| c.topic == contract.name)
            .map(|c| c.overrides.clone())
            .unwrap_or_default();

        // Compaction is structural: a state topic under `delete` loses its
        // map whatever the deployment's size, so it is checked even when no
        // deployment numbers were supplied.
        if contract.kind == TopicKind::CompactedState {
            let policy = overrides.get(CLEANUP_POLICY);
            if policy.map(String::as_str) != Some(COMPACT) {
                drift.push(TopicDrift::CleanupPolicy {
                    topic: contract.name.to_string(),
                    actual: policy.cloned(),
                    purpose: contract.purpose,
                });
            }
        }
        if let Some(spec) = spec {
            if partitions.get() != spec.partitions {
                drift.push(TopicDrift::PartitionCount {
                    topic: contract.name.to_string(),
                    expected: spec.partitions,
                    actual: partitions.get(),
                    purpose: contract.purpose,
                });
            }
            if entry.replication_factor != spec.replicas {
                drift.push(TopicDrift::ReplicationFactor {
                    topic: contract.name.to_string(),
                    expected: spec.replicas,
                    actual: entry.replication_factor,
                });
            }
            if contract.kind == TopicKind::Wal {
                let expected = spec.configs.get(RETENTION_MS);
                let actual = overrides.get(RETENTION_MS);
                if expected != actual {
                    drift.push(TopicDrift::Retention {
                        topic: contract.name.to_string(),
                        expected: expected.and_then(|v| v.parse().ok()).unwrap_or_default(),
                        actual: actual.cloned(),
                    });
                }
            }
        }

        observed.push(ObservedTopic {
            name: contract.name.to_string(),
            partitions,
            replication_factor: entry.replication_factor,
            overrides,
        });
    }

    (observed, drift)
}
