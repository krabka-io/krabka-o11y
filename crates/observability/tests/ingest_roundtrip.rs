//! End-to-end logs ingest against a real broker: a Loki push over HTTP, then
//! the Kafka WAL, then the compactor, then an object-store block, then a query
//! that reads that block back.
//!
//! The metrics and traces slices each boot a `krabka-broker` in-process and
//! drive their signal through it (`crates/metrics/tests/ingest_roundtrip.rs`,
//! `crates/traces/tests/support/mod.rs`). The logs slice had no such test: every
//! other suite here either hands the distributor an `InMemoryWalSink` or hands
//! the compactor a recorded batch, so nothing checked that the records the
//! distributor produces are the records a real Kafka implementation hands back.
//! This is that check, and it is an ordinary `bazel test` target because the
//! broker runs in this process rather than in a container.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

use assert2::{assert, check};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use krabka_blockstore::{
    BlockKey, TimeRange, labels, read_log_block_from_object_store, series_fingerprint,
};
use krabka_broker::{Broker, BrokerConfig};
use krabka_client_admin::{AdminClient, CreateTopicSpec};
use krabka_client_consumer::{AutoOffsetReset, Consumer};
use krabka_observability::{
    KafkaLogWalConsumer, Offset, PartitionIndex, QuerierIndexSource, Role, ServiceConfig,
    ServiceDependencies, WalLogRecord, WalPosition, build_service_dependencies,
    build_service_router, decode_kafka_wal_record, metrics::ServiceMetrics,
    run_compactor_until_idle, wal_consumer_metrics::WalConsumerMetrics,
};
use krabka_units::{days, secs};
use object_store::{local::LocalFileSystem, path::Path as ObjectPath};
use serde_json::{Value, json};
use tower::ServiceExt as _;

/// The tenant every step of the round trip is scoped to.
const TENANT: &str = "tenant-a";

/// Where the compactor writes blocks and shard indexes, and where the querier
/// reads them from.
const INDEX_PREFIX: &str = "observability/logs";

/// How long a step that waits on the broker may take before the test gives up.
///
/// A produce is acknowledged before the consumer's group has been assigned its
/// partition, so the first poll of a fresh group can legitimately return
/// nothing. That is a retry, not a failure; this bounds the retrying.
const BROKER_DEADLINE: Duration = Duration::from_secs(20);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn loki_push_reaches_a_block_through_the_broker_wal_and_answers_a_query() {
    let broker_dir = tempfile::tempdir().expect("broker tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(broker_dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    let wal_topic = ServiceConfig::default().wal_topic;
    create_wal_topic(&bootstrap, &wal_topic).await;

    // 1. The real HTTP door. Same router the distributor role serves, same
    //    Loki push body a client would send, and a sink that produces to the
    //    broker rather than to a vector in this process.
    let distributor_root = tempfile::tempdir().expect("distributor data root");
    let mut distributor_config = roundtrip_config(
        Role::Distributor,
        distributor_root.path().to_path_buf(),
        &bootstrap,
        &wal_topic,
        None,
    );
    distributor_config.reject_old_samples_max_age = days(36_500);
    let dependencies =
        build_service_dependencies(&distributor_config, WalConsumerMetrics::unregistered())
            .await
            .expect("distributor dependencies");
    let response = build_service_router(&distributor_config, dependencies, None)
        .await
        .expect("distributor router")
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("content-type", "application/json")
                .header("X-Scope-OrgID", TENANT)
                .body(Body::from(push_body().to_string()))
                .expect("push request"),
        )
        .await
        .expect("push response");
    assert!(response.status() == StatusCode::NO_CONTENT);

    // 2. The WAL, read back off the broker with an ordinary consumer. This is
    //    the step every other suite here stubs out.
    let wal_records = consume_wal_records(&bootstrap, &wal_topic, 2).await;
    assert!(wal_records == expected_wal_records());

    // 3. The compactor, consuming that same topic and writing to an object
    //    store.
    let object_dir = tempfile::tempdir().expect("object store tempdir");
    let store = LocalFileSystem::new_with_prefix(object_dir.path()).expect("object store");
    // The compactor's consumer and its object store both record into one
    // bundle, so the scrape at the end of this test reads what a real
    // deployment's `/metrics` would show for this exact round trip.
    let metrics = ServiceMetrics::new();
    let consumer = KafkaLogWalConsumer::connect(
        bootstrap.clone(),
        "krabka-observability-roundtrip-compactor",
        wal_topic.clone(),
    )
    .await
    .expect("compactor consumer connect")
    .with_metrics(metrics.wal_consumer.clone());
    let compactor_root = tempfile::tempdir().expect("compactor data root");
    let descriptors = run_compactor_until_idle(
        &roundtrip_config(
            Role::BlockBuilder,
            compactor_root.path().to_path_buf(),
            &bootstrap,
            &wal_topic,
            None,
        ),
        ServiceDependencies::default()
            .with_wal_consumer(consumer)
            .with_metrics(metrics.clone()),
        Some(&store),
    )
    .await
    .expect("compactor run");

    check_compactor_instruments_moved(&metrics, &wal_topic).await;

    let expected_key = BlockKey::new(TENANT, 0, 0, 1, TimeRange::new(10, 20).expect("time range"));
    assert!(descriptors.len() == 1);
    check!(descriptors[0].key == expected_key);
    // Two series, not one: the distributor's level discovery puts
    // `detected_level` on the entry whose line says "error" and on no other, so
    // the two entries of one pushed stream land as two label sets.
    check!(
        descriptors[0].fingerprints
            == BTreeSet::from([
                series_fingerprint(&labels([
                    ("app", "api"),
                    ("detected_level", "error"),
                    ("env", "prod"),
                    ("service_name", "api"),
                ])),
                series_fingerprint(&labels([
                    ("app", "api"),
                    ("env", "prod"),
                    ("service_name", "api"),
                ])),
            ])
    );

    // 4. The block itself, read straight out of the object store.
    let rows =
        read_log_block_from_object_store(&store, &ObjectPath::from(INDEX_PREFIX), &expected_key)
            .await
            .expect("read compacted block");
    check!(
        rows.iter().map(|row| row.line.as_str()).collect::<Vec<_>>()
            == vec!["api error", "api recovered"]
    );

    // 5. The querier, pointed at the shard indexes the compactor just wrote,
    //    answering a LogQL query over that block.
    let querier_root = tempfile::tempdir().expect("querier data root");
    let querier_config = roundtrip_config(
        Role::Querier,
        querier_root.path().to_path_buf(),
        &bootstrap,
        &wal_topic,
        Some(format!("file://{}", object_dir.path().display())),
    );
    let dependencies =
        build_service_dependencies(&querier_config, WalConsumerMetrics::unregistered())
            .await
            .expect("querier dependencies");
    let app = build_service_router(&querier_config, dependencies, None)
        .await
        .expect("querier router");
    let ready_deadline = Instant::now() + BROKER_DEADLINE;
    loop {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .expect("ready request"),
            )
            .await
            .expect("ready response");
        if response.status() == StatusCode::OK {
            break;
        }
        assert!(
            Instant::now() < ready_deadline,
            "broker-backed querier authorizer did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30&direction=forward")
                .header("X-Scope-OrgID", TENANT)
                .body(Body::empty())
                .expect("query request"),
        )
        .await
        .expect("query response");

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    check!(body["status"] == json!("success"));
    check!(body["data"]["resultType"] == json!("streams"));
    check!(
        body["data"]["result"]
            == json!([{
                // The structured metadata pushed with the entry survives the
                // whole trip -- HTTP, WAL, Parquet block, query -- and comes
                // back folded into the stream's labels, which is where Loki's
                // default encoding puts it.
                "stream": {
                    "app": "api",
                    "detected_level": "error",
                    "env": "prod",
                    "service_name": "api",
                    "trace_id": "abc",
                },
                "values": [["10", "api error"]],
            }])
    );
}

/// The compactor's instruments, read back through the registry the `/metrics`
/// exporter serves, after a real broker and a real object store have been
/// driven end to end.
///
/// An instrument that is registered and never incremented scrapes exactly like
/// one that was never registered, and neither is visible from the recording
/// call. Only a scrape after real work separates them, so this reads the
/// encoded text rather than the handles.
async fn check_compactor_instruments_moved(metrics: &ServiceMetrics, wal_topic: &str) {
    let mut buffer = String::new();
    let registry = metrics.registry.lock().await;
    prometheus_client::encoding::text::encode(&mut buffer, &registry).expect("encode the registry");

    for needle in [
        // Two WAL records were produced above, and the compactor read both.
        format!(
            "krabka_logs_wal_consumer_records_total{{topic=\"{wal_topic}\",partition=\"0\"}} 2"
        ),
        format!(
            "krabka_logs_wal_consumer_last_consumed_offset{{topic=\"{wal_topic}\",partition=\"0\"}} 1"
        ),
        "krabka_logs_wal_consumer_polls_total{outcome=\"records\"} 1".to_string(),
        // The loop polls again and gets nothing, which is what a caught-up
        // consumer looks like and is why the empty outcome has a series of its
        // own.
        "krabka_logs_wal_consumer_polls_total{outcome=\"empty\"} 1".to_string(),
        // One compaction pass ran, it succeeded, and it wrote one block.
        "krabka_logs_compaction_runs_total{status=\"ok\"} 1".to_string(),
        "krabka_logs_compaction_duration_seconds_count 1".to_string(),
        "krabka_logs_compaction_blocks_total 1".to_string(),
    ] {
        assert!(buffer.contains(&needle), "missing {needle} in:\n{buffer}");
    }

    check!(
        !buffer.contains("krabka_logs_compaction_runs_total{status=\"error\"}"),
        "a clean round trip must not move the compaction error series:\n{buffer}"
    );

    // The logs distributor produces its WAL records without a timestamp, so
    // every record arrives with none and the delay histogram takes no
    // observation. A count above zero here means an unstamped record reached
    // the histogram and reported the age of the Unix epoch.
    check!(
        buffer.contains("krabka_logs_wal_consumer_receive_delay_seconds_count 0"),
        "an unstamped WAL record must contribute no delay observation:\n{buffer}"
    );

    // The object-store instruments are absent here on purpose: this test hands
    // the compactor a store of its own, and the decorator is applied where the
    // role builds its store. The metrics round trip covers that path.
    check!(
        !buffer.contains("krabka_logs_objstore_operations_total"),
        "an injected store bypasses the decorator, so it counts nothing:\n{buffer}"
    );
}

/// The push body: two entries in one stream, one of them carrying structured
/// metadata, so the WAL assertion covers both shapes of record.
fn push_body() -> Value {
    json!({
        "streams": [{
            "stream": {"app": "api", "env": "prod"},
            "values": [
                ["10", "api error", {"trace_id": "abc"}],
                ["20", "api recovered"],
            ],
        }],
    })
}

/// What the broker must hand back for that body, whole records rather than a
/// chain of field assertions.
///
/// `position` is the WAL coordinate the decoder stamps on, so this also pins
/// that both entries landed on one partition in the order they were pushed.
fn expected_wal_records() -> Vec<WalLogRecord> {
    vec![
        WalLogRecord {
            tenant: TENANT.to_string(),
            labels: labels([
                ("app", "api"),
                ("detected_level", "error"),
                ("env", "prod"),
                ("service_name", "api"),
            ]),
            timestamp_ns: 10,
            line: "api error".to_string(),
            structured_metadata: BTreeMap::from([("trace_id".to_string(), "abc".to_string())]),
            position: Some(WalPosition {
                partition: PartitionIndex(0),
                offset: Offset(0),
            }),
        },
        WalLogRecord {
            tenant: TENANT.to_string(),
            labels: labels([("app", "api"), ("env", "prod"), ("service_name", "api")]),
            timestamp_ns: 20,
            line: "api recovered".to_string(),
            structured_metadata: BTreeMap::new(),
            position: Some(WalPosition {
                partition: PartitionIndex(0),
                offset: Offset(1),
            }),
        },
    ]
}

/// One `ServiceConfig` shape for both roles the round trip runs.
fn roundtrip_config(
    target: Role,
    data_root: std::path::PathBuf,
    bootstrap: &str,
    wal_topic: &str,
    object_store_url: Option<String>,
) -> ServiceConfig {
    ServiceConfig {
        target,
        listen_addr: "127.0.0.1:0".parse().expect("listen addr"),
        object_store_url,
        wal_bootstrap_server: Some(bootstrap.to_string()),
        wal_topic: wal_topic.to_string(),
        wal_group_id: "krabka-observability-roundtrip".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreShards,
        tenant: None,
        index_prefix: Some(INDEX_PREFIX.to_string()),
        ..ServiceConfig::default()
    }
}

async fn create_wal_topic(bootstrap: &str, wal_topic: &str) {
    let mut admin = AdminClient::connect(&[bootstrap.to_string()])
        .await
        .expect("admin connect");
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: wal_topic.to_string(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::default(),
            }],
            secs(10),
        )
        .await
        .expect("create observability wal topic");
}

/// Polls the WAL topic until `expected` records have arrived, then decodes them
/// with the same decoder the compactor uses.
async fn consume_wal_records(
    bootstrap: &str,
    wal_topic: &str,
    expected: usize,
) -> Vec<WalLogRecord> {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("krabka-observability-roundtrip-inspect")
        .client_id("krabka-observability-roundtrip-inspect")
        .subscribe([wal_topic.to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("inspect consumer");

    let mut decoded = Vec::new();
    let deadline = Instant::now() + BROKER_DEADLINE;
    while Instant::now() < deadline && decoded.len() < expected {
        for record in consumer.poll(secs(1)).await.expect("poll wal topic") {
            let value = record.value.expect("wal record value");
            decoded.push(
                decode_kafka_wal_record(
                    &value,
                    PartitionIndex(record.partition),
                    Offset(record.offset),
                )
                .expect("decode wal record"),
            );
        }
    }
    assert!(decoded.len() == expected);
    decoded
}

async fn json_body(response: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), 256 * 1024)
        .await
        .expect("read response body");
    serde_json::from_slice(&body).expect("response is json")
}
