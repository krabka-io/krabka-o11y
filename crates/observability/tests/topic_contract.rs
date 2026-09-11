//! The topic contract against a real broker.
//!
//! A mock admin client would prove that this crate builds the requests it
//! means to build. It would not prove the thing the contract is about: that a
//! topic which already exists keeps its own configuration when a second
//! `CreateTopics` arrives, and that the describe afterwards is what finds it.
//! So every case here boots a `krabka-broker` in process and drives the real
//! `CreateTopics`, `Metadata` and `DescribeConfigs` path.

use std::{collections::BTreeMap, time::Duration};

use assert2::{assert, check};
use axum::body::Bytes;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use krabka_client_admin::{AdminClient, CreateTopicSpec};
use krabka_client_producer::{Producer, ProducerRecord, partition_for_key};
use krabka_observability::{
    ServiceConfig,
    topic_contract::{
        ALL_TOPICS, CLEANUP_POLICY, COMPACT, LOGS_WAL_TOPIC, METRICS_HA_TOPIC,
        METRICS_RULER_STATE_TOPIC, METRICS_WAL_TOPIC, PROFILES_WAL_TOPIC, PartitionCount,
        RETENTION_MS, TRACES_WAL_TOPIC, TopicContractError, TopicDrift, TopicKind, TopicSettings,
        check_topics, ensure_topics, shard_count, verify_topics,
    },
};
use krabka_units::{millis, minutes, secs};

/// How long a `send` may take before the test gives up on the broker.
const SEND_DEADLINE: Duration = Duration::from_secs(20);

struct TestBroker {
    bootstrap: String,
    _broker: BrokerHandle,
    _dir: tempfile::TempDir,
}

async fn start_broker() -> TestBroker {
    let dir = tempfile::tempdir().expect("broker tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .expect("broker start");
    TestBroker {
        bootstrap: broker.listen_addr().to_string(),
        _broker: broker,
        _dir: dir,
    }
}

async fn admin(bootstrap: &str) -> AdminClient {
    AdminClient::connect(&[bootstrap.to_string()])
        .await
        .expect("admin connect")
}

fn settings(wal_partitions: i32, state_partitions: i32) -> TopicSettings {
    TopicSettings {
        wal_partitions: PartitionCount::new(wal_partitions).expect("positive"),
        state_partitions: PartitionCount::new(state_partitions).expect("positive"),
        replication_factor: 1,
        wal_retention: minutes(15),
    }
}

async fn create_raw(bootstrap: &str, name: &str, partitions: i32, configs: &[(&str, &str)]) {
    let outcomes = admin(bootstrap)
        .await
        .create_topics(
            &[CreateTopicSpec {
                name: name.to_string(),
                partitions,
                replicas: 1,
                configs: configs
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect(),
            }],
            secs(10),
        )
        .await
        .expect("create topic");
    assert!(outcomes.iter().all(|o| o.error.is_none()), "{outcomes:?}");
}

async fn live_partitions(bootstrap: &str, name: &str) -> i32 {
    admin(bootstrap)
        .await
        .metadata(&[name])
        .await
        .expect("metadata")
        .topics
        .iter()
        .find(|topic| topic.name == name)
        .expect("topic in metadata")
        .partition_count
}

async fn live_overrides(bootstrap: &str, name: &str) -> BTreeMap<String, String> {
    admin(bootstrap)
        .await
        .describe_configs(&[name])
        .await
        .expect("describe configs")
        .into_iter()
        .find(|config| config.topic == name)
        .expect("topic in describe configs")
        .overrides
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provisioning_creates_all_six_topics_with_the_configuration_the_contract_states() {
    let broker = start_broker().await;
    let mut admin = admin(&broker.bootstrap).await;
    let settings = settings(3, 2);

    // Nothing exists yet, and checking without creating says so rather than
    // reporting a pass over an empty cluster.
    let refused = verify_topics(&mut admin, &ALL_TOPICS, &settings)
        .await
        .expect_err("no topic exists yet");
    let TopicContractError::Contract { drift } = refused else {
        panic!("expected a contract error, got {refused}");
    };
    check!(drift.len() == ALL_TOPICS.len());
    check!(
        drift
            .iter()
            .all(|d| matches!(d, TopicDrift::Missing { .. }))
    );

    let report = ensure_topics(&mut admin, &ALL_TOPICS, &settings)
        .await
        .expect("provision the contract topics");

    check!(report.advisory == Vec::new());
    for contract in &ALL_TOPICS {
        let expected = match contract.kind {
            TopicKind::Wal => 3,
            TopicKind::CompactedState => 2,
        };
        check!(
            live_partitions(&broker.bootstrap, contract.name).await == expected,
            "{} partitions",
            contract.name
        );
        check!(
            report.partitions(contract.name) == PartitionCount::new(expected).ok(),
            "{} shard count in the report",
            contract.name
        );

        let overrides = live_overrides(&broker.bootstrap, contract.name).await;
        let expected_overrides = match contract.kind {
            TopicKind::Wal => BTreeMap::from([(RETENTION_MS.to_string(), "900000".to_string())]),
            TopicKind::CompactedState => {
                BTreeMap::from([(CLEANUP_POLICY.to_string(), COMPACT.to_string())])
            }
        };
        check!(overrides == expected_overrides, "{}", contract.name);
    }

    // The names the signal crates use are the names that were created.
    let created: Vec<&str> = ALL_TOPICS.iter().map(|topic| topic.name).collect();
    check!(
        created
            == vec![
                METRICS_WAL_TOPIC,
                METRICS_HA_TOPIC,
                METRICS_RULER_STATE_TOPIC,
                TRACES_WAL_TOPIC,
                PROFILES_WAL_TOPIC,
                LOGS_WAL_TOPIC,
            ]
    );

    // A second run is a no-op that still validates.
    let again = ensure_topics(&mut admin, &ALL_TOPICS, &settings)
        .await
        .expect("provisioning is repeatable");
    check!(again.observed == report.observed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_that_already_has_the_wrong_partition_count_is_refused_not_silently_kept() {
    let broker = start_broker().await;
    // The dangerous case: something created the WAL topic first with one
    // partition, which is what a broker's own auto-creation would do.
    create_raw(&broker.bootstrap, METRICS_WAL_TOPIC, 1, &[]).await;

    let mut admin = admin(&broker.bootstrap).await;
    let error = ensure_topics(&mut admin, &ALL_TOPICS, &settings(3, 2))
        .await
        .expect_err("a one-partition WAL topic cannot serve three shards");

    let TopicContractError::Contract { drift } = error else {
        panic!("expected a contract error, got {error}");
    };
    check!(
        drift.contains(&TopicDrift::PartitionCount {
            topic: METRICS_WAL_TOPIC.to_string(),
            expected: 3,
            actual: 1,
            purpose: ALL_TOPICS[0].purpose,
        }),
        "{drift:?}"
    );
    let message = drift
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    check!(message.contains("re-maps every key"), "{message}");

    // The create succeeded as a no-op, which is exactly why the describe has
    // to run: the wrong count is still there.
    check!(live_partitions(&broker.bootstrap, METRICS_WAL_TOPIC).await == 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_state_topic_that_is_not_compacted_is_refused() {
    let broker = start_broker().await;
    // One state topic left at the broker's default policy, one set to
    // `delete` outright. Both lose the state they hold at the retention
    // window, and the broker reports overrides only, so the first is visible
    // as an absent key rather than as `delete`.
    create_raw(&broker.bootstrap, METRICS_HA_TOPIC, 2, &[]).await;
    create_raw(
        &broker.bootstrap,
        METRICS_RULER_STATE_TOPIC,
        2,
        &[(CLEANUP_POLICY, "delete")],
    )
    .await;

    let mut admin = admin(&broker.bootstrap).await;
    let error = ensure_topics(&mut admin, &ALL_TOPICS, &settings(3, 2))
        .await
        .expect_err("an uncompacted state topic loses HA and alert state");

    let TopicContractError::Contract { drift } = error else {
        panic!("expected a contract error, got {error}");
    };
    check!(
        drift.contains(&TopicDrift::CleanupPolicy {
            topic: METRICS_HA_TOPIC.to_string(),
            actual: None,
            purpose: ALL_TOPICS[1].purpose,
        }),
        "{drift:?}"
    );
    check!(
        drift.contains(&TopicDrift::CleanupPolicy {
            topic: METRICS_RULER_STATE_TOPIC.to_string(),
            actual: Some("delete".to_string()),
            purpose: ALL_TOPICS[2].purpose,
        }),
        "{drift:?}"
    );
    check!(drift.iter().all(TopicDrift::is_fatal));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_different_retention_window_is_reported_and_the_role_still_starts() {
    let broker = start_broker().await;
    create_raw(
        &broker.bootstrap,
        LOGS_WAL_TOPIC,
        3,
        &[(RETENTION_MS, "60000")],
    )
    .await;

    let mut admin = admin(&broker.bootstrap).await;
    let report = ensure_topics(&mut admin, &[ALL_TOPICS[5]], &settings(3, 2))
        .await
        .expect("a short retention window is an operator's choice, not corruption");

    check!(
        report.advisory
            == vec![TopicDrift::Retention {
                topic: LOGS_WAL_TOPIC.to_string(),
                expected: 900_000,
                actual: Some("60000".to_string()),
            }]
    );
    check!(!report.advisory[0].is_fatal());
    check!(
        report.advisory[0]
            .to_string()
            .contains("how far the block-builder may fall behind"),
        "{}",
        report.advisory[0]
    );
    // The topic keeps the operator's window; provisioning does not overwrite
    // it behind their back.
    check!(
        live_overrides(&broker.bootstrap, LOGS_WAL_TOPIC).await
            == BTreeMap::from([(RETENTION_MS.to_string(), "60000".to_string())])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn roles_that_start_at_the_same_time_all_provision_successfully() {
    let broker = start_broker().await;
    let settings = settings(3, 2);

    // Eight roles start together against an empty cluster. One `CreateTopics`
    // wins each topic; the rest read TOPIC_ALREADY_EXISTS and must treat it
    // as success rather than as a startup failure.
    let starts = (0..8).map(|_| {
        let bootstrap = broker.bootstrap.clone();
        tokio::spawn(async move {
            let mut admin = admin(&bootstrap).await;
            ensure_topics(&mut admin, &ALL_TOPICS, &settings)
                .await
                .map(|report| report.observed.len())
        })
    });
    let outcomes = futures_util::future::join_all(starts).await;

    for outcome in outcomes {
        let counted = outcome
            .expect("provisioning task")
            .expect("concurrent start");
        check!(counted == ALL_TOPICS.len());
    }
    for contract in &ALL_TOPICS {
        let expected = match contract.kind {
            TopicKind::Wal => 3,
            TopicKind::CompactedState => 2,
        };
        check!(
            live_partitions(&broker.bootstrap, contract.name).await == expected,
            "{}",
            contract.name
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_shard_a_key_lands_on_is_decided_by_the_topic_partition_count() {
    let broker = start_broker().await;
    let mut admin = admin(&broker.bootstrap).await;
    ensure_topics(&mut admin, &[ALL_TOPICS[0]], &settings(4, 1))
        .await
        .expect("provision the metrics WAL");

    // Nothing configures a shard count. It is read back from the same broker
    // metadata the producer routes against.
    let shards = shard_count(&mut admin, METRICS_WAL_TOPIC)
        .await
        .expect("shard count");
    check!(shards == PartitionCount::new(4).expect("positive"));

    let producer = Producer::builder()
        .bootstrap(&broker.bootstrap)
        .build()
        .await
        .expect("producer");

    let mut landed = std::collections::BTreeSet::new();
    for series in 0_u32..24 {
        let key = Bytes::from(format!("tenant-a\0{series}").into_bytes());
        let acknowledged = producer
            .send(ProducerRecord {
                topic: METRICS_WAL_TOPIC.to_string(),
                key: Some(key.clone()),
                value: Some(Bytes::from_static(b"sample")),
                ..ProducerRecord::default()
            })
            .await;
        let metadata = tokio::time::timeout(SEND_DEADLINE, acknowledged)
            .await
            .expect("the broker acknowledges the send")
            .expect("the producer is still running")
            .expect("produce");

        check!(
            metadata.partition == partition_for_key(&key, shards.get()),
            "key {series} landed on partition {} and the shard count says otherwise",
            metadata.partition
        );
        landed.insert(metadata.partition);
    }

    check!(
        landed.len() > 1,
        "the keys must spread across the partitions, or the check above proves nothing"
    );

    // A role learns the same number without being told it. `check_topics`
    // states no partition count, so there is nothing for it to disagree with.
    let role_view = check_topics(&mut admin, &[ALL_TOPICS[0]])
        .await
        .expect("a role starts against a provisioned topic");
    check!(role_view.partitions(METRICS_WAL_TOPIC) == Some(shards));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settings_that_no_broker_could_satisfy_are_rejected_before_any_request() {
    let broker = start_broker().await;
    let mut admin = admin(&broker.bootstrap).await;

    let error = ensure_topics(
        &mut admin,
        &ALL_TOPICS,
        &TopicSettings {
            wal_retention: millis(0),
            ..settings(1, 1)
        },
    )
    .await
    .expect_err("a zero WAL window may drop a record the moment it is written");
    check!(matches!(error, TopicContractError::InvalidRetention { .. }));

    // Nothing was created on the way to that refusal.
    let still_absent = verify_topics(&mut admin, &ALL_TOPICS, &settings(1, 1))
        .await
        .expect_err("nothing was created");
    let TopicContractError::Contract { drift } = still_absent else {
        panic!("expected a contract error, got {still_absent}");
    };
    check!(
        drift
            .iter()
            .all(|d| matches!(d, TopicDrift::Missing { .. })),
        "{drift:?}"
    );
}

#[test]
fn the_logs_service_default_wal_topic_is_the_contract_topic() {
    // The logs WAL topic name is an operator-settable flag, so it is the one
    // of the six that the contract cannot own outright. Its default must
    // still be the topic the contract provisions, or a default deployment
    // writes to a topic nothing created.
    check!(ServiceConfig::default().wal_topic == LOGS_WAL_TOPIC);
}
