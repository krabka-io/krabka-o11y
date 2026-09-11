//! The audit layer against a real broker: three events through
//! `AuditService::start`, then the audit topic read back with an ordinary
//! consumer and the partition checked with the offline verifier.
//!
//! The broker runs in this process, the way `ingest_roundtrip` runs it.

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use assert2::{assert, check};
use krabka_audit::{AuditRecord, ChainState, TrustedKeys, to_ocsf, verify_partition_dir};
use krabka_broker::{Broker, BrokerConfig};
use krabka_client_admin::{AdminClient, CreateTopicSpec};
use krabka_client_consumer::{AutoOffsetReset, Consumer};
use krabka_observability::{
    CancellationToken, PartitionIndex,
    audit::{
        AuditArgs, AuditEvent, AuditOutcome, AuditService, DEFAULT_AUDIT_CHECKPOINT_EVERY,
        DEFAULT_AUDIT_QUEUE_CAPACITY, DEFAULT_AUDIT_SPOOL_MAX, EpochMs, MECHANISM_BASIC,
        OPERATION_DELETE_REQUEST_CREATE, OPERATION_RULE_GROUP_DELETE, OPERATION_TENANT_WRITE,
        ProductInfo, RESOURCE_DELETE_REQUEST, RESOURCE_RULE_GROUP, RESOURCE_TENANT,
        admin_operation, authorization_denied, krabka_product, principal, resource,
        source_endpoint,
    },
};
use krabka_units::secs;
use serde_json::{Value, json};

/// The audit topic. Its second partition is the one this process writes, so
/// the test also shows the sink does not write to the partition a key or a
/// partitioner picks.
const AUDIT_TOPIC: &str = "krabka-observability-audit";

/// The partition the audit layer writes.
const AUDIT_PARTITION: i32 = 1;

/// The epoch-millisecond time of the first event.
const START_MS: i64 = 1_700_000_000_000;

/// How long a step that waits on the broker may take before the test gives up.
const BROKER_DEADLINE: Duration = Duration::from_secs(20);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn audit_events_reach_one_partition_of_the_topic_in_order_on_a_valid_chain() {
    let broker_dir = tempfile::tempdir().expect("broker tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(broker_dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    create_audit_topic(&bootstrap).await;

    let args = AuditArgs {
        topic: Some(AUDIT_TOPIC.to_owned()),
        bootstrap: None,
        partition: PartitionIndex(AUDIT_PARTITION),
        spool_dir: None,
        spool_max: DEFAULT_AUDIT_SPOOL_MAX,
        queue_capacity: DEFAULT_AUDIT_QUEUE_CAPACITY,
        checkpoint_every: DEFAULT_AUDIT_CHECKPOINT_EVERY,
        signing_key_path: None,
        signing_key_id: None,
    };
    let shutdown = CancellationToken::new();
    // No `--audit-bootstrap`: the layer uses the bootstrap the service gives.
    let (handle, writer) =
        AuditService::start(&args, product(), Some(&bootstrap), None, shutdown.clone())
            .await
            .expect("the audit layer starts")
            .into_parts();
    let events = events();
    for event in &events {
        handle.emit(event.clone());
    }

    let consumed = consume_audit_records(&bootstrap, events.len()).await;

    shutdown.cancel();
    writer
        .expect("an enabled layer has a writer")
        .await
        .expect("the writer does not panic");

    let mut chain = ChainState::new();
    let expected: Vec<ConsumedRecord> = events
        .iter()
        .zip(0_i64..)
        .map(|(event, offset)| {
            let mut record = AuditRecord::from_event(event, &product());
            let (seq, prev_head) = chain.extend(&record.value);
            record.push_chain_headers(seq, &prev_head);
            ConsumedRecord {
                partition: AUDIT_PARTITION,
                offset,
                ocsf: to_ocsf(event, &product()),
                headers: headers(&record),
            }
        })
        .collect();
    check!(consumed == expected);

    // The values the upstream crate writes, stated here as data, so a change
    // of OCSF mapping shows up in this suite and not only in `krabka-audit`.
    check!(
        consumed
            .iter()
            .map(|record| (
                record.ocsf["class_uid"].clone(),
                record.ocsf["time"].clone(),
                record.ocsf["actor"]["user"].clone(),
                record.headers["event_class"].as_str(),
                record.headers["status"].as_str(),
                record.headers["seq"].as_str(),
            ))
            .collect::<Vec<_>>()
            == vec![
                (
                    json!(6003),
                    json!(START_MS),
                    json!({"name": "alice", "type": "basic"}),
                    "api_activity",
                    "success",
                    "0",
                ),
                (
                    json!(3003),
                    json!(START_MS + 1),
                    json!({"name": "alice", "type": "basic"}),
                    "authorization",
                    "denied",
                    "1",
                ),
                (
                    json!(6003),
                    json!(START_MS + 2),
                    json!({"name": "alice", "type": "basic"}),
                    "api_activity",
                    "failure",
                    "2",
                ),
            ]
    );
    check!(consumed[0].ocsf["api"]["operation"] == json!("delete_request.create"));
    check!(
        consumed[0].ocsf["resources"]
            == json!([
                {"type": "tenant", "name": "tenant-a"},
                {"type": "delete_request", "name": "request-1"},
            ])
    );

    // The broker's own partition files, read by the verifier the broker's
    // audit trail uses. No signing key is set, so no record is anchored.
    let report = verify_partition_dir(
        &broker_dir
            .path()
            .join(format!("{AUDIT_TOPIC}-{AUDIT_PARTITION}")),
        &TrustedKeys::default(),
    )
    .expect("the verifier reads the partition");
    check!(
        (
            report.ok,
            report.records.0,
            report.checkpoints.0,
            report.unanchored_records.0,
        ) == (true, 3, 0, 3)
    );
    check!(handle.dropped() == 0);
}

/// One record as the consumer hands it back.
#[derive(Debug, PartialEq)]
struct ConsumedRecord {
    partition: i32,
    offset: i64,
    ocsf: Value,
    headers: BTreeMap<String, String>,
}

fn product() -> ProductInfo {
    krabka_product("krabka-observability", "0.0.0-test")
}

fn events() -> Vec<AuditEvent> {
    let alice = || principal("alice", MECHANISM_BASIC);
    let client = || source_endpoint("10.0.0.7:51000".parse().expect("the address is valid"));
    vec![
        admin_operation(
            alice(),
            client(),
            OPERATION_DELETE_REQUEST_CREATE,
            vec![
                resource(RESOURCE_TENANT, "tenant-a"),
                resource(RESOURCE_DELETE_REQUEST, "request-1"),
            ],
            AuditOutcome::Success,
            EpochMs(START_MS),
        ),
        authorization_denied(
            alice(),
            client(),
            RESOURCE_TENANT,
            "tenant-b",
            OPERATION_TENANT_WRITE,
            EpochMs(START_MS + 1),
        ),
        admin_operation(
            alice(),
            client(),
            OPERATION_RULE_GROUP_DELETE,
            vec![resource(RESOURCE_RULE_GROUP, "team-a/alerts")],
            AuditOutcome::Failure,
            EpochMs(START_MS + 2),
        ),
    ]
}

fn headers(record: &AuditRecord) -> BTreeMap<String, String> {
    record
        .headers
        .iter()
        .map(|(key, value)| (key.clone(), String::from_utf8_lossy(value).into_owned()))
        .collect()
}

async fn create_audit_topic(bootstrap: &str) {
    let mut admin = AdminClient::connect(&[bootstrap.to_string()])
        .await
        .expect("admin connect");
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: AUDIT_TOPIC.to_string(),
                partitions: 2,
                replicas: 1,
                configs: BTreeMap::default(),
            }],
            secs(10),
        )
        .await
        .expect("create the audit topic");
}

/// Polls the audit topic until `expected` records have arrived.
async fn consume_audit_records(bootstrap: &str, expected: usize) -> Vec<ConsumedRecord> {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("krabka-observability-audit-inspect")
        .client_id("krabka-observability-audit-inspect")
        .subscribe([AUDIT_TOPIC.to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("inspect consumer");

    let mut records = Vec::new();
    let deadline = Instant::now() + BROKER_DEADLINE;
    while Instant::now() < deadline && records.len() < expected {
        for record in consumer.poll(secs(1)).await.expect("poll the audit topic") {
            let ocsf = record.value.expect("an audit record has a value");
            records.push(ConsumedRecord {
                partition: record.partition,
                offset: record.offset,
                ocsf: serde_json::from_slice(&ocsf).expect("the value is OCSF JSON"),
                headers: record
                    .headers
                    .into_iter()
                    .map(|header| {
                        let text = header.value.expect("an audit header has a value");
                        (header.key, String::from_utf8_lossy(&text).into_owned())
                    })
                    .collect(),
            });
        }
    }
    assert!(records.len() == expected);
    records
}
