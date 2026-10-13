use std::collections::BTreeMap;

use assert2::{assert, check};
use krabka_client_admin::{
    Config, ConfigEntry, ConfigResource, ConfigSource, ConfigType, KafkaError, TopicMetadata,
    TopicMetadataEntry,
};
use krabka_units::{minutes, secs};
use maplit::btreemap;

use super::{
    ALL_TOPICS, CLEANUP_POLICY, COMPACT, METRICS_HA_TOPIC, METRICS_WAL_TOPIC, ObservedTopic,
    PartitionCount, RETENTION_MS, TopicContractError, TopicDrift, TopicExpectation, TopicKind,
    TopicSettings, desired_specs, inspect_topics,
};

fn settings(wal_partitions: i32, state_partitions: i32, replicas: i32) -> TopicSettings {
    TopicSettings {
        wal_partitions: PartitionCount::new(wal_partitions).expect("positive"),
        state_partitions: PartitionCount::new(state_partitions).expect("positive"),
        replication_factor: replicas,
        wal_retention: minutes(15),
    }
}

fn entry(name: &str, partitions: i32, replicas: i32) -> TopicMetadataEntry {
    TopicMetadataEntry {
        name: name.to_string(),
        topic_id: None,
        partition_count: partitions,
        replication_factor: replicas,
        error: None,
    }
}

struct ConfigPair {
    name: &'static str,
    value: &'static str,
}

struct TopicConfigOverride {
    resource: ConfigResource,
    config: Result<Config, KafkaError>,
}

fn config_pair(name: &'static str, value: &'static str) -> ConfigPair {
    ConfigPair { name, value }
}

fn overrides(name: &str, pairs: &[ConfigPair]) -> TopicConfigOverride {
    let mut entries = BTreeMap::new();
    for pair in pairs {
        entries.insert(
            pair.name.to_string(),
            ConfigEntry {
                name: pair.name.to_string(),
                value: Some(pair.value.to_string()),
                source: ConfigSource::DynamicTopicConfig,
                is_sensitive: false,
                is_read_only: false,
                synonyms: Vec::new(),
                config_type: ConfigType::Unknown,
                documentation: None,
            },
        );
    }
    TopicConfigOverride {
        resource: ConfigResource::topic(name),
        config: Ok(Config { entries }),
    }
}

fn topic_configs(
    entries: impl IntoIterator<Item = TopicConfigOverride>,
) -> BTreeMap<ConfigResource, Result<Config, KafkaError>> {
    let mut configs = BTreeMap::new();
    for entry in entries {
        configs.insert(entry.resource, entry.config);
    }
    configs
}

fn metadata(entries: Vec<TopicMetadataEntry>) -> TopicMetadata {
    TopicMetadata {
        controller_id: 1,
        topics: entries,
    }
}

#[test]
fn a_wal_topic_asks_for_retention_and_a_state_topic_asks_for_compaction() {
    let specs = desired_specs(&ALL_TOPICS, &settings(4, 2, 3));

    for pair in ALL_TOPICS.iter().zip(&specs) {
        let contract = pair.0;
        let spec = pair.1;
        check!(spec.name == contract.name);
        check!(spec.replicas == 3);
        match contract.kind {
            TopicKind::Wal => {
                check!(spec.partitions == 4);
                check!(
                    spec.configs == btreemap! { RETENTION_MS.to_string() => "900000".to_string() },
                    "a WAL topic states its retention window and nothing else"
                );
            }
            TopicKind::CompactedState => {
                check!(spec.partitions == 2);
                check!(
                    spec.configs == btreemap! { CLEANUP_POLICY.to_string() => COMPACT.to_string() },
                    "a compacted topic must not carry a retention window"
                );
            }
        }
    }
}

#[test]
fn a_matching_broker_reports_no_drift_and_the_live_shard_count() {
    let contracts = [ALL_TOPICS[0], ALL_TOPICS[1]];
    let settings = settings(4, 2, 1);
    let specs = desired_specs(&contracts, &settings);
    let metadata = metadata(vec![
        entry(METRICS_WAL_TOPIC, 4, 1),
        entry(METRICS_HA_TOPIC, 2, 1),
    ]);
    let configs = topic_configs([
        overrides(METRICS_WAL_TOPIC, &[config_pair(RETENTION_MS, "900000")]),
        overrides(METRICS_HA_TOPIC, &[config_pair(CLEANUP_POLICY, COMPACT)]),
    ]);

    let inspection = inspect_topics(
        &contracts,
        &TopicExpectation::Deployment(&specs),
        &metadata,
        &configs,
    );
    let observed = inspection.observed;
    let drift = inspection.drift;

    assert!(drift == Vec::new());
    assert!(
        observed
            == vec![
                ObservedTopic {
                    name: METRICS_WAL_TOPIC.to_string(),
                    partitions: PartitionCount::new(4).expect("positive"),
                    replication_factor: 1,
                    overrides: btreemap! { RETENTION_MS.to_string() => "900000".to_string() },
                },
                ObservedTopic {
                    name: METRICS_HA_TOPIC.to_string(),
                    partitions: PartitionCount::new(2).expect("positive"),
                    replication_factor: 1,
                    overrides: btreemap! { CLEANUP_POLICY.to_string() => COMPACT.to_string() },
                },
            ]
    );
}

#[test]
fn a_wrong_partition_count_is_fatal_and_names_both_counts() {
    let contracts = [ALL_TOPICS[0]];
    let settings = settings(4, 1, 1);
    let specs = desired_specs(&contracts, &settings);
    let metadata = metadata(vec![entry(METRICS_WAL_TOPIC, 1, 1)]);
    let configs = topic_configs([overrides(
        METRICS_WAL_TOPIC,
        &[config_pair(RETENTION_MS, "900000")],
    )]);

    let drift = inspect_topics(
        &contracts,
        &TopicExpectation::Deployment(&specs),
        &metadata,
        &configs,
    )
    .drift;

    assert!(
        drift
            == vec![TopicDrift::PartitionCount {
                topic: METRICS_WAL_TOPIC.to_string(),
                expected: 4,
                actual: 1,
                purpose: ALL_TOPICS[0].purpose,
            }]
    );
    check!(drift[0].is_fatal());
    let message = drift[0].to_string();
    check!(message.contains("has 1 partitions"), "{message}");
    check!(message.contains("requires 4"), "{message}");
}

#[test]
fn an_uncompacted_state_topic_is_fatal_whether_the_policy_is_wrong_or_absent() {
    let contracts = [ALL_TOPICS[1]];
    let settings = settings(1, 1, 1);
    let specs = desired_specs(&contracts, &settings);
    let metadata = metadata(vec![entry(METRICS_HA_TOPIC, 1, 1)]);

    let wrong = inspect_topics(
        &contracts,
        &TopicExpectation::Deployment(&specs),
        &metadata,
        &topic_configs([overrides(
            METRICS_HA_TOPIC,
            &[config_pair(CLEANUP_POLICY, "delete")],
        )]),
    )
    .drift;
    check!(
        wrong
            == vec![TopicDrift::CleanupPolicy {
                topic: METRICS_HA_TOPIC.to_string(),
                actual: Some("delete".to_string()),
                purpose: ALL_TOPICS[1].purpose,
            }]
    );

    // The broker reports overrides only, so a topic left at the broker
    // default reports no `cleanup.policy` at all. That default is `delete`,
    // so an absent override is the same fault and not a pass.
    let absent = inspect_topics(
        &contracts,
        &TopicExpectation::Deployment(&specs),
        &metadata,
        &topic_configs([overrides(METRICS_HA_TOPIC, &[])]),
    )
    .drift;
    check!(
        absent
            == vec![TopicDrift::CleanupPolicy {
                topic: METRICS_HA_TOPIC.to_string(),
                actual: None,
                purpose: ALL_TOPICS[1].purpose,
            }]
    );
    check!(absent[0].is_fatal());
    check!(
        absent[0]
            .to_string()
            .contains("<unset, so the broker default>"),
        "{}",
        absent[0]
    );
}

#[test]
fn retention_and_replication_differences_are_reported_but_not_fatal() {
    let contracts = [ALL_TOPICS[0]];
    let settings = settings(1, 1, 3);
    let specs = desired_specs(&contracts, &settings);
    let metadata = metadata(vec![entry(METRICS_WAL_TOPIC, 1, 1)]);
    let configs = topic_configs([overrides(
        METRICS_WAL_TOPIC,
        &[config_pair(RETENTION_MS, "60000")],
    )]);

    let inspection = inspect_topics(
        &contracts,
        &TopicExpectation::Deployment(&specs),
        &metadata,
        &configs,
    );
    let observed = inspection.observed;
    let drift = inspection.drift;

    assert!(
        drift
            == vec![
                TopicDrift::ReplicationFactor {
                    topic: METRICS_WAL_TOPIC.to_string(),
                    expected: 3,
                    actual: 1,
                },
                TopicDrift::Retention {
                    topic: METRICS_WAL_TOPIC.to_string(),
                    expected: 900_000,
                    actual: Some("60000".to_string()),
                },
            ]
    );
    check!(!drift[0].is_fatal());
    check!(!drift[1].is_fatal());
    check!(observed.len() == 1, "the topic is still usable");
}

#[test]
fn an_absent_or_unreadable_topic_is_fatal() {
    let contracts = [ALL_TOPICS[0], ALL_TOPICS[1]];
    let settings = settings(1, 1, 1);
    let specs = desired_specs(&contracts, &settings);
    // A broker answers `Metadata` for a topic it does not hold with
    // UNKNOWN_TOPIC_OR_PARTITION rather than by omitting the topic, so that
    // code reads as absent. Any other per-topic error is a topic this call
    // cannot check, which is its own fault.
    let metadata = metadata(vec![
        TopicMetadataEntry {
            error: Some(KafkaError {
                code: 3,
                name: "UNKNOWN_TOPIC_OR_PARTITION",
                message: None,
            }),
            ..entry(METRICS_WAL_TOPIC, 0, 0)
        },
        TopicMetadataEntry {
            error: Some(KafkaError {
                code: 29,
                name: "TOPIC_AUTHORIZATION_FAILED",
                message: None,
            }),
            ..entry(METRICS_HA_TOPIC, 0, 0)
        },
    ]);

    let inspection = inspect_topics(
        &contracts,
        &TopicExpectation::Deployment(&specs),
        &metadata,
        &BTreeMap::new(),
    );
    let observed = inspection.observed;
    let drift = inspection.drift;

    assert!(observed == Vec::new());
    assert!(
        drift
            == vec![
                TopicDrift::Missing {
                    topic: METRICS_WAL_TOPIC.to_string(),
                    purpose: ALL_TOPICS[0].purpose,
                },
                TopicDrift::Unreadable {
                    topic: METRICS_HA_TOPIC.to_string(),
                    error: "TOPIC_AUTHORIZATION_FAILED".to_string(),
                },
            ]
    );
    check!(drift.iter().all(TopicDrift::is_fatal));
}

#[test]
fn a_topic_the_broker_omits_entirely_is_also_absent() {
    let contracts = [ALL_TOPICS[0]];
    let settings = settings(1, 1, 1);
    let specs = desired_specs(&contracts, &settings);

    let inspection = inspect_topics(
        &contracts,
        &TopicExpectation::Deployment(&specs),
        &metadata(Vec::new()),
        &BTreeMap::new(),
    );
    let observed = inspection.observed;
    let drift = inspection.drift;

    assert!(observed == Vec::new());
    assert!(
        drift
            == vec![TopicDrift::Missing {
                topic: METRICS_WAL_TOPIC.to_string(),
                purpose: ALL_TOPICS[0].purpose,
            }]
    );
}

#[test]
fn a_partition_count_must_be_positive() {
    check!(PartitionCount::new(1).expect("positive").get() == 1);
    check!(matches!(
        PartitionCount::new(0),
        Err(TopicContractError::InvalidPartitionCount { count: 0 })
    ));
    check!(matches!(
        PartitionCount::new(-1),
        Err(TopicContractError::InvalidPartitionCount { count: -1 })
    ));
}

#[test]
fn settings_reject_a_broker_that_could_never_hold_the_data() {
    check!(TopicSettings::single_broker().validate().is_ok());
    check!(matches!(
        TopicSettings {
            replication_factor: 0,
            ..TopicSettings::single_broker()
        }
        .validate(),
        Err(TopicContractError::InvalidReplicationFactor { factor: 0 })
    ));
    check!(matches!(
        TopicSettings {
            wal_retention: secs(0),
            ..TopicSettings::single_broker()
        }
        .validate(),
        Err(TopicContractError::InvalidRetention { .. })
    ));
}

#[test]
fn every_contract_topic_has_a_distinct_name() {
    let mut names: Vec<&str> = ALL_TOPICS.iter().map(|topic| topic.name).collect();
    names.sort_unstable();
    let distinct = names.len();
    names.dedup();
    assert!(names.len() == distinct, "{names:?}");
}

#[test]
fn a_structure_check_reads_the_shard_count_instead_of_comparing_it() {
    // A role does not state a partition count, so it cannot disagree with the
    // one the deployment set. Whatever the live count is, the role takes it
    // and starts -- but a state topic that is not compacted still stops it.
    let contracts = [ALL_TOPICS[0], ALL_TOPICS[1]];
    let metadata = metadata(vec![
        entry(METRICS_WAL_TOPIC, 12, 3),
        entry(METRICS_HA_TOPIC, 5, 3),
    ]);
    let compacted = topic_configs([
        overrides(METRICS_WAL_TOPIC, &[config_pair(RETENTION_MS, "60000")]),
        overrides(METRICS_HA_TOPIC, &[config_pair(CLEANUP_POLICY, COMPACT)]),
    ]);

    let inspection = inspect_topics(
        &contracts,
        &TopicExpectation::Structure,
        &metadata,
        &compacted,
    );
    let observed = inspection.observed;
    let drift = inspection.drift;

    assert!(drift == Vec::new());
    check!(
        observed.iter().map(|t| t.partitions).collect::<Vec<_>>()
            == vec![
                PartitionCount::new(12).expect("positive"),
                PartitionCount::new(5).expect("positive"),
            ],
        "the live counts come back for the role to use"
    );

    let uncompacted = topic_configs([
        overrides(METRICS_WAL_TOPIC, &[config_pair(RETENTION_MS, "60000")]),
        overrides(METRICS_HA_TOPIC, &[]),
    ]);
    let drift = inspect_topics(
        &contracts,
        &TopicExpectation::Structure,
        &metadata,
        &uncompacted,
    )
    .drift;
    assert!(
        drift
            == vec![TopicDrift::CleanupPolicy {
                topic: METRICS_HA_TOPIC.to_string(),
                actual: None,
                purpose: ALL_TOPICS[1].purpose,
            }]
    );
}

/// `provision_topics` logs every advisory difference with `topic` as a field
/// and the difference as the message, so an operator can filter the warnings
/// for one topic. The field comes from [`TopicDrift::topic`], and the enum is
/// `#[non_exhaustive]`: a variant added without a `topic` arm is a compile
/// error, but a variant added to the *wrong* arm is not, and would file the
/// warning under another topic's name.
#[test]
fn every_difference_names_the_topic_it_is_about_and_says_whether_it_is_fatal() {
    struct TopicDifferenceCase {
        drift: TopicDrift,
        topic: &'static str,
        fatal: bool,
    }
    let cases = [
        TopicDifferenceCase {
            drift: TopicDrift::Missing {
                topic: "krabka.logs.wal".to_string(),
                purpose: "the log write-ahead log",
            },
            topic: "krabka.logs.wal",
            fatal: true,
        },
        TopicDifferenceCase {
            drift: TopicDrift::Unreadable {
                topic: "krabka.logs.state".to_string(),
                error: "TOPIC_AUTHORIZATION_FAILED".to_string(),
            },
            topic: "krabka.logs.state",
            fatal: true,
        },
        TopicDifferenceCase {
            drift: TopicDrift::PartitionCount {
                topic: "krabka.metrics.wal".to_string(),
                expected: 4,
                actual: 8,
                purpose: "the metrics write path",
            },
            topic: "krabka.metrics.wal",
            fatal: true,
        },
        TopicDifferenceCase {
            drift: TopicDrift::CleanupPolicy {
                topic: "krabka.metrics.ha".to_string(),
                actual: None,
                purpose: "the HA dedup state",
            },
            topic: "krabka.metrics.ha",
            fatal: true,
        },
        TopicDifferenceCase {
            drift: TopicDrift::ReplicationFactor {
                topic: "krabka.traces.wal".to_string(),
                expected: 3,
                actual: 1,
            },
            topic: "krabka.traces.wal",
            fatal: false,
        },
        TopicDifferenceCase {
            drift: TopicDrift::Retention {
                topic: "krabka.profiles.wal".to_string(),
                expected: 900_000,
                actual: Some("60000".to_string()),
            },
            topic: "krabka.profiles.wal",
            fatal: false,
        },
    ];

    for case in cases {
        check!(case.drift.topic() == case.topic, "{:?}", case.drift);
        check!(case.drift.is_fatal() == case.fatal, "{:?}", case.drift);
        check!(
            case.drift.to_string().contains(case.topic),
            "the message names the topic too, so a line read without its fields still says which"
        );
    }
}
