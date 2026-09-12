//! The logs WAL against a real broker, beyond the one round trip.
//!
//! `ingest_roundtrip` drives one push through the broker WAL, the block
//! builder and the querier. That covers the happy path and the two ACL
//! denials. It leaves out every contract that only a live Kafka group can
//! prove: the raw sink and consumer pair, a broker-side quota, the hot tail a
//! querier answers from before anything is compacted, the websocket tail on a
//! served listener, the OTLP door, two tenants on one topic, a hot and cold
//! merge, a block builder that restarts on its committed offset, and a record
//! a native Kafka client produced with no distributor in front of it.
//!
//! Every case here runs the broker in this process, so these are ordinary
//! `bazel test` targets and need no container.

mod support;

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use assert2::{assert, check};
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use bytes::Bytes;
use futures_util::StreamExt as _;
use krabka_blockstore::{
    BlockDescriptor, LabelIndex, LogBlockIndex as BlockIndex, labels, write_log_index_manifest,
};
use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use krabka_client_admin::{AdminClient, CreateTopicSpec, QuotaOp};
use krabka_client_producer::{Acks, Header, Producer, ProducerRecord};
use krabka_observability::{
    BufferedLogHotTail, KafkaLogWalConsumer, KafkaLogWalSink, LogWalConsumer as _, LogWalSink as _,
    Offset, PartitionIndex, QuerierIndexSource, Role, ServiceConfig, ServiceDependencies,
    WalLogRecord, WalPosition, build_service_dependencies, build_service_router,
    poll_log_hot_tail_once, run_compactor_until_idle, run_compactor_until_shutdown,
    serve_service_listener, wal_consumer_metrics::WalConsumerMetrics,
};
use krabka_units::{millis, secs};
use serde_json::{Value, json};
use support::{expected_loki_mixed_stats_with, test_service_config};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest as _};
use tower::ServiceExt as _;

/// The tenant every case writes as, unless it names another one.
const TENANT: &str = "tenant-a";

/// Where the block builder writes blocks and shard indexes.
const INDEX_PREFIX: &str = "observability/logs";

/// How long a step that waits on the broker may take before the test gives up.
///
/// A produce is acknowledged before a fresh consumer group has been assigned
/// its partition, so the first poll of one can legitimately return nothing.
/// That is a retry, not a failure; this bounds the retrying.
const BROKER_DEADLINE: Duration = Duration::from_secs(30);

/// How long one block-builder run gets before its shutdown signal fires.
const COMPACTOR_RUN: Duration = Duration::from_millis(750);

// ---------------------------------------------------------------------------
// The raw sink and consumer pair.
// ---------------------------------------------------------------------------

/// The sink writes a record, and the consumer reads back the same record with
/// the position the broker gave it.
///
/// Nothing above this pair is involved: no HTTP door, no service config. It is
/// the contract the distributor and the block builder both depend on, and it
/// is the one step `ingest_roundtrip` replaces with an ordinary consumer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_kafka_wal_sink_and_consumer_agree_on_a_live_record() {
    let live = LiveBroker::start("__krabka_observability_logs_wal_live").await;

    let record = WalLogRecord {
        tenant: TENANT.to_string(),
        labels: labels([("app", "api"), ("env", "prod")]),
        timestamp_ns: 20_000_000,
        line: "api live wal error".to_string(),
        structured_metadata: BTreeMap::from([("trace_id".to_string(), "abc123".to_string())]),
        position: None,
    };
    KafkaLogWalSink::connect(live.bootstrap.clone(), live.topic.clone())
        .await
        .expect("wal sink")
        .append(record.clone())
        .await
        .expect("append wal record");

    let mut consumer = live.wal_consumer("live").await;
    let hot_tail = BufferedLogHotTail::default();
    let decoded = poll_until_decoded(&mut consumer, &hot_tail).await;

    assert!(decoded == 1);
    let position = WalPosition {
        partition: PartitionIndex(0),
        offset: Offset(0),
    };
    assert!(
        hot_tail.records()
            == vec![WalLogRecord {
                position: Some(position),
                ..record
            }]
    );
    consumer
        .commit_compacted(position)
        .await
        .expect("commit the compacted position");

    live.shutdown().await;
}

// ---------------------------------------------------------------------------
// The broker's own limits.
// ---------------------------------------------------------------------------

/// A broker-side `producer_byte_rate` quota refuses the push, and nothing
/// reaches the WAL.
///
/// The quota lives on the broker, not in this process, so only a real broker
/// can answer whether the distributor reads it and stops in front of the
/// append rather than after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_producer_byte_rate_quota_refuses_a_push_before_the_wal_append() {
    let live = LiveBroker::start("__krabka_observability_logs_wal_quota").await;
    let mut admin = live.admin().await;
    let outcome = admin
        .alter_user_quotas(
            TENANT,
            &[QuotaOp::Set {
                key: "producer_byte_rate".to_string(),
                value: 32.0,
            }],
            false,
        )
        .await
        .expect("set the tenant quota");
    assert!(outcome.is_none());

    let data_root = TempDir::new().expect("data root");
    let distributor = live
        .router(live.config(Role::Distributor, &data_root))
        .await;
    let response = distributor
        .oneshot(push_request(
            TENANT,
            &fixture_timestamp_ns(20_000_000),
            "api quota blocked because this line is larger than the configured tenant byte rate",
        ))
        .await
        .expect("push response");

    assert!(response.status() == StatusCode::TOO_MANY_REQUESTS);
    let body = json_body(response).await;
    check!(body["errorType"] == "rate_limited");
    check!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains("producer_byte_rate"))
    );

    let mut consumer = live.wal_consumer("quota").await;
    let hot_tail = BufferedLogHotTail::default();
    let decoded = poll_log_hot_tail_once(&mut consumer, &hot_tail, millis(250))
        .await
        .expect("poll the live wal");
    check!(decoded == 0);
    check!(hot_tail.records().is_empty());

    live.shutdown().await;
}

// ---------------------------------------------------------------------------
// The live tail.
// ---------------------------------------------------------------------------

/// The querier answers out of the live WAL before anything is compacted.
///
/// No block builder runs here, and no object store is configured. Every line
/// in the answer therefore came off the broker through the querier's own hot
/// tail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_querier_answers_from_the_live_wal_tail_before_anything_is_compacted() {
    let live = LiveBroker::start("__krabka_observability_logs_wal_hot_only").await;
    let data_root = TempDir::new().expect("data root");

    let mut label_index = LabelIndex::default();
    label_index.insert_series(TENANT, labels([("app", "api"), ("env", "prod")]));
    write_log_index_manifest(data_root.path(), &label_index, &BlockIndex::default())
        .expect("write the manifest");

    KafkaLogWalSink::connect(live.bootstrap.clone(), live.topic.clone())
        .await
        .expect("wal sink")
        .append(WalLogRecord {
            tenant: TENANT.to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20_000_000,
            line: "api live tail error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .expect("append the wal record");

    let querier = live.router(live.config(Role::Querier, &data_root)).await;
    let body = query_until_answered(
        &querier,
        TENANT,
        "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=20000000",
    )
    .await;

    assert!(
        body == json!({
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [{
                    "stream": {
                        "app": "api",
                        "detected_level": "unknown",
                        "env": "prod",
                    },
                    "values": [["20000000", "api live tail error"]],
                }],
                "stats": expected_loki_mixed_stats_with(0, 0, 1, 0),
            },
        })
    );

    live.shutdown().await;
}

/// A websocket tail on a served listener streams a line pushed after the
/// client connected.
///
/// `tail.rs` proves the same for an `InMemoryWalSink` handed straight to the
/// router. This proves it for the whole configured path: a distributor writing
/// a real broker, and a querier that `serve_service_listener` put on a real
/// socket.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_websocket_tail_on_a_served_listener_streams_the_live_wal() {
    let live = LiveBroker::start("__krabka_observability_logs_wal_tail_listener").await;
    let data_root = TempDir::new().expect("data root");
    write_log_index_manifest(
        data_root.path(),
        &LabelIndex::default(),
        &BlockIndex::default(),
    )
    .expect("write the manifest");

    let distributor = live
        .router(live.config(Role::Distributor, &data_root))
        .await;

    let mut querier_config = live.config(Role::Querier, &data_root);
    querier_config.wal_group_id = "krabka-observability-wal-tail-listener-querier".to_string();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let addr = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let dependencies = dependencies(&querier_config).await;
        serve_service_listener(listener, querier_config, dependencies, None)
            .await
            .expect("serve the querier");
    });
    wait_until_ready_at(addr).await;

    let timestamp = fixture_timestamp_ns(20_000_000);
    let end = timestamp.parse::<i64>().expect("timestamp") + 10_000_000;
    let mut request = format!(
        "ws://{addr}/loki/api/v1/tail?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22tail%22&start=0&end={end}"
    )
    .into_client_request()
    .expect("tail request");
    request
        .headers_mut()
        .insert("X-Scope-OrgID", TENANT.parse().expect("tenant header"));
    let (mut socket, response) = connect_async(request).await.expect("tail websocket");
    assert!(response.status() == StatusCode::SWITCHING_PROTOCOLS);

    push_log(
        &distributor,
        TENANT,
        &timestamp,
        "api live websocket tail error",
    )
    .await;

    let message = tokio::time::timeout(Duration::from_secs(10), socket.next())
        .await
        .expect("a tail frame arrived")
        .expect("the tail socket stayed open")
        .expect("the tail frame decoded");
    let frame: Value =
        serde_json::from_str(message.to_text().expect("a text frame")).expect("frame json");

    assert!(
        frame
            == json!({
                "streams": [{
                    "stream": {
                        "app": "api",
                        "detected_level": "error",
                        "env": "prod",
                        "service_name": "api",
                    },
                    "values": [[timestamp, "api live websocket tail error"]],
                }],
            })
    );

    server.abort();
    live.shutdown().await;
}

// ---------------------------------------------------------------------------
// The whole loop.
// ---------------------------------------------------------------------------

/// An OTLP log reaches a query answer through the live WAL and a block.
///
/// `distributor_otlp` proves the decode against an in-memory sink. Nothing
/// proved that the record it produces survives the broker, the block builder
/// and the querier with its resource and scope attributes intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_otlp_log_reaches_a_query_answer_through_the_live_wal_and_a_block() {
    let live = LiveBroker::start("__krabka_observability_logs_wal_otlp_loop").await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");

    let distributor = live
        .router(live.config(Role::Distributor, &data_root))
        .await;
    let timestamp = current_unix_second_ns().to_string();
    let response = distributor
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", TENANT)
                .header("content-type", "application/json")
                .body(Body::from(otlp_body(&timestamp).to_string()))
                .expect("otlp request"),
        )
        .await
        .expect("otlp response");
    assert!(response.status() == StatusCode::NO_CONTENT);

    let descriptors = live.compact(&data_root, &object_root, "otlp-loop").await;
    assert!(descriptors.len() == 1);
    check!(descriptors[0].key.first_offset == 0);
    check!(descriptors[0].key.last_offset == 0);

    let querier = live
        .router(live.querier_config(&data_root, &object_root, "otlp-loop"))
        .await;
    let body = query_until_answered(
        &querier,
        TENANT,
        &format!(
            "/loki/api/v1/query_range?query=%7Bservice_name%3D%22checkout%22%7D&start=0&end={}&direction=forward",
            timestamp.parse::<i64>().expect("timestamp") + 10_000_000
        ),
    )
    .await;

    check!(
        body.pointer("/data/result/0/stream")
            == Some(&json!({
                "deployment_environment": "prod",
                "detected_level": "unknown",
                "instrumentation_scope": "api",
                "service_name": "checkout",
                "status": "500",
                "trace_id": "abc123",
            }))
    );
    check!(
        body.pointer("/data/result/0/values")
            == Some(&json!([[timestamp, "checkout otlp loop error"]]))
    );

    live.shutdown().await;
}

/// Two tenants share one WAL topic, and neither one ever reads the other's
/// lines.
///
/// One topic is the deployment shape the contract states, so the isolation is
/// a property of the block builder's per-tenant block keys and of the
/// querier's tenant filter, not of the storage layout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_tenants_sharing_one_wal_topic_never_read_each_other_lines() {
    let live = LiveBroker::start("__krabka_observability_logs_wal_tenant_loop").await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");

    let distributor = live
        .router(live.config(Role::Distributor, &data_root))
        .await;
    let seeded = [
        (
            "tenant-a",
            fixture_timestamp_ns(20_000_000),
            "tenant a shared wal error",
        ),
        (
            "tenant-b",
            fixture_timestamp_ns(21_000_000),
            "tenant b shared wal error",
        ),
    ];
    for (tenant, timestamp, line) in &seeded {
        push_log(&distributor, tenant, timestamp, line).await;
    }

    let descriptors = live.compact(&data_root, &object_root, "tenant-loop").await;
    assert!(descriptors.len() == 2);

    let querier = live
        .router(live.querier_config(&data_root, &object_root, "tenant-loop"))
        .await;
    let end = seeded[1].1.parse::<i64>().expect("timestamp") + 10_000_000;
    for (tenant, timestamp, line) in &seeded {
        let body = query_until_answered(
            &querier,
            tenant,
            &format!(
                "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end={end}&direction=forward"
            ),
        )
        .await;
        check!(
            body.pointer("/data/result/0/values") == Some(&json!([[timestamp, line]])),
            "{tenant}"
        );
        let other = seeded.iter().find(|(name, _, _)| name != tenant);
        check!(
            !body
                .to_string()
                .contains(other.expect("the other tenant").2),
            "{tenant} read the other tenant's line"
        );
    }

    live.shutdown().await;
}

/// One query returns a compacted line and an uncompacted one, in order.
///
/// `querier_object_store` proves the merge with a block written by hand and an
/// in-memory tail. Here the block came out of a real block-builder run and the
/// tail is the live topic, so the boundary the merge turns on is the offset the
/// block builder actually committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_querier_merges_a_compacted_block_with_the_uncompacted_live_tail() {
    let live = LiveBroker::start("__krabka_observability_logs_wal_hot_cold_loop").await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");

    let distributor = live
        .router(live.config(Role::Distributor, &data_root))
        .await;
    let compacted = fixture_timestamp_ns(10_000_000);
    let live_line = fixture_timestamp_ns(20_000_000);
    push_log(&distributor, TENANT, &compacted, "api compacted error").await;

    let descriptors = live.compact(&data_root, &object_root, "hot-cold").await;
    assert!(descriptors.len() == 1);
    check!(descriptors[0].key.first_offset == 0);
    check!(descriptors[0].key.last_offset == 0);

    push_log(&distributor, TENANT, &live_line, "api live tail error").await;

    let querier = live
        .router(live.querier_config(&data_root, &object_root, "hot-cold"))
        .await;
    let body = query_until_answered(
        &querier,
        TENANT,
        &format!(
            "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end={}&direction=forward",
            live_line.parse::<i64>().expect("timestamp") + 10_000_000
        ),
    )
    .await;

    assert!(
        body.pointer("/data/result/0/values")
            == Some(&json!([
                [compacted, "api compacted error"],
                [live_line, "api live tail error"],
            ]))
    );

    live.shutdown().await;
}

/// A block builder that restarts resumes on the offset its group committed.
///
/// The two runs are separate `ServiceDependencies`, so the second one knows
/// nothing except what the broker holds for the group. Its block therefore
/// starts at offset 1, and the first record is not written twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_block_builder_resumes_from_the_committed_wal_offset() {
    let live = LiveBroker::start("__krabka_observability_logs_wal_restart").await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");

    let distributor = live
        .router(live.config(Role::Distributor, &data_root))
        .await;
    let first = fixture_timestamp_ns(10_000_000);
    let second = fixture_timestamp_ns(20_000_000);
    push_log(&distributor, TENANT, &first, "api first restart error").await;

    let config = live.compactor_config(&data_root, &object_root, "restart");
    let first_run = run_compactor_for(&config, COMPACTOR_RUN).await;
    assert!(first_run.len() == 1);
    check!(first_run[0].key.first_offset == 0);
    check!(first_run[0].key.last_offset == 0);

    push_log(&distributor, TENANT, &second, "api second restart error").await;

    let restarted = live.compactor_config(&data_root, &object_root, "restart");
    let second_run = run_compactor_for(&restarted, COMPACTOR_RUN).await;
    assert!(second_run.len() == 1);
    check!(second_run[0].key.first_offset == 1);
    check!(second_run[0].key.last_offset == 1);

    let querier = live
        .router(live.querier_config(&data_root, &object_root, "restart"))
        .await;
    let body = query_until_answered(
        &querier,
        TENANT,
        &format!(
            "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end={}&direction=forward",
            second.parse::<i64>().expect("timestamp") + 10_000_000
        ),
    )
    .await;

    assert!(
        body.pointer("/data/result/0/values")
            == Some(&json!([
                [first, "api first restart error"],
                [second, "api second restart error"],
            ]))
    );

    live.shutdown().await;
}

/// A record produced by a native Kafka client reaches a query answer.
///
/// No distributor writes this one. The headers are the whole contract, so the
/// case shows that the WAL topic is an interface anyone with a Kafka client
/// can write, not a private encoding of one producer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_log_produced_by_a_native_kafka_client_reaches_a_query_answer() {
    let live = LiveBroker::start("__krabka_observability_logs_wal_native_loop").await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");

    produce_native_kafka_log(&live, "20000000", "api native kafka error").await;

    let descriptors = live.compact(&data_root, &object_root, "native-loop").await;
    assert!(descriptors.len() == 1);
    check!(descriptors[0].key.first_offset == 0);
    check!(descriptors[0].key.last_offset == 0);

    let querier = live
        .router(live.querier_config(&data_root, &object_root, "native-loop"))
        .await;
    let body = query_until_answered(
        &querier,
        TENANT,
        "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=20000000",
    )
    .await;

    check!(
        body.pointer("/data/result/0/stream")
            == Some(&json!({
                "app": "api",
                "detected_level": "unknown",
                "env": "prod",
                "trace_id": "abc123",
            }))
    );
    check!(
        body.pointer("/data/result/0/values")
            == Some(&json!([["20000000", "api native kafka error"]]))
    );

    live.shutdown().await;
}

// ---------------------------------------------------------------------------
// The broker, and the roles around it.
// ---------------------------------------------------------------------------

/// One in-process broker with one WAL topic.
struct LiveBroker {
    handle: BrokerHandle,
    bootstrap: String,
    topic: String,
    _dir: TempDir,
}

impl LiveBroker {
    async fn start(topic: &str) -> Self {
        let dir = TempDir::new().expect("broker tempdir");
        let handle = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
            .await
            .expect("broker start");
        let bootstrap = handle.listen_addr().to_string();
        let live = Self {
            handle,
            bootstrap,
            topic: topic.to_string(),
            _dir: dir,
        };
        live.admin()
            .await
            .create_topics(
                &[CreateTopicSpec {
                    name: live.topic.clone(),
                    partitions: 1,
                    replicas: 1,
                    configs: BTreeMap::default(),
                }],
                secs(10),
            )
            .await
            .expect("create the wal topic");
        live
    }

    async fn admin(&self) -> AdminClient {
        AdminClient::connect(std::slice::from_ref(&self.bootstrap))
            .await
            .expect("admin connect")
    }

    async fn wal_consumer(&self, group: &str) -> KafkaLogWalConsumer {
        KafkaLogWalConsumer::connect(
            self.bootstrap.clone(),
            format!("krabka-observability-wal-live-{group}"),
            self.topic.clone(),
        )
        .await
        .expect("wal consumer")
    }

    /// A role's configuration, pointed at this broker and this topic.
    fn config(&self, target: Role, data_root: &TempDir) -> ServiceConfig {
        ServiceConfig {
            wal_bootstrap_server: Some(self.bootstrap.clone()),
            wal_topic: self.topic.clone(),
            wal_group_id: format!("krabka-observability-wal-live-{target:?}"),
            ..test_service_config(target, data_root.path().to_path_buf())
        }
    }

    /// A block builder that writes blocks and shard indexes into
    /// `object_root`.
    fn compactor_config(
        &self,
        data_root: &TempDir,
        object_root: &TempDir,
        group: &str,
    ) -> ServiceConfig {
        ServiceConfig {
            object_store_url: Some(format!("file://{}", object_root.path().display())),
            index_prefix: Some(INDEX_PREFIX.to_string()),
            wal_group_id: format!("krabka-observability-wal-live-{group}-block-builder"),
            ..self.config(Role::BlockBuilder, data_root)
        }
    }

    /// A querier that reads the shard indexes the block builder wrote.
    fn querier_config(
        &self,
        data_root: &TempDir,
        object_root: &TempDir,
        group: &str,
    ) -> ServiceConfig {
        ServiceConfig {
            object_store_url: Some(format!("file://{}", object_root.path().display())),
            index_prefix: Some(INDEX_PREFIX.to_string()),
            querier_index_source: QuerierIndexSource::TenantObjectStoreShards,
            wal_group_id: format!("krabka-observability-wal-live-{group}-querier"),
            ..self.config(Role::Querier, data_root)
        }
    }

    async fn router(&self, config: ServiceConfig) -> Router {
        let dependencies = dependencies(&config).await;
        build_service_router(&config, dependencies, None)
            .await
            .expect("build the role router")
    }

    /// Runs one block builder over everything the topic holds.
    async fn compact(
        &self,
        data_root: &TempDir,
        object_root: &TempDir,
        group: &str,
    ) -> Vec<BlockDescriptor> {
        let config = self.compactor_config(data_root, object_root, group);
        let dependencies = dependencies(&config).await;
        run_compactor_until_idle(&config, dependencies, None)
            .await
            .expect("block builder run")
    }

    async fn shutdown(self) {
        self.handle.shutdown().await;
    }
}

async fn dependencies(config: &ServiceConfig) -> ServiceDependencies {
    build_service_dependencies(config, WalConsumerMetrics::unregistered())
        .await
        .expect("service dependencies")
}

/// Runs a block builder for a fixed wall-clock span and returns its blocks.
async fn run_compactor_for(config: &ServiceConfig, duration: Duration) -> Vec<BlockDescriptor> {
    run_compactor_until_shutdown(
        config,
        dependencies(config).await,
        None,
        // A fixed run duration, not a progress poll: the point is that the
        // run ends where it ends and the next one picks up from the committed
        // offset.
        tokio::time::sleep(duration),
    )
    .await
    .expect("block builder run")
}

async fn produce_native_kafka_log(live: &LiveBroker, timestamp_ns: &str, line: &str) {
    let producer = Producer::builder()
        .bootstrap(live.bootstrap.clone())
        .client_id("krabka-observability-wal-native-producer")
        .acks(Acks::All)
        .build()
        .await
        .expect("native Kafka producer");
    producer
        .send(ProducerRecord {
            topic: live.topic.clone(),
            partition: Some(0),
            key: Some(Bytes::from(format!("{TENANT}:api"))),
            value: Some(Bytes::from(line.to_string())),
            headers: vec![
                kafka_header("krabka-tenant", TENANT),
                kafka_header("krabka-log-timestamp-ns", timestamp_ns),
                kafka_header("krabka-log-label-app", "api"),
                kafka_header("krabka-log-label-env", "prod"),
                kafka_header("krabka-log-metadata-trace_id", "abc123"),
            ],
            timestamp_ms: None,
        })
        .await
        .await
        .expect("native Kafka delivery channel")
        .expect("native Kafka produce");
    producer.flush().await.expect("flush the producer");
    producer.close().await.expect("close the producer");
}

fn kafka_header(key: &str, value: &str) -> Header {
    Header {
        key: key.to_string(),
        value: Some(Bytes::from(value.to_string())),
    }
}

// ---------------------------------------------------------------------------
// Requests, and waiting for answers.
// ---------------------------------------------------------------------------

fn push_request(tenant: &str, timestamp_ns: &str, line: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/loki/api/v1/push")
        .header("X-Scope-OrgID", tenant)
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "streams": [{
                    "stream": { "app": "api", "env": "prod" },
                    "values": [[timestamp_ns, line]],
                }],
            })
            .to_string(),
        ))
        .expect("push request")
}

async fn push_log(distributor: &Router, tenant: &str, timestamp_ns: &str, line: &str) {
    let response = distributor
        .clone()
        .oneshot(push_request(tenant, timestamp_ns, line))
        .await
        .expect("push response");
    assert!(response.status() == StatusCode::NO_CONTENT);
}

fn otlp_body(timestamp_ns: &str) -> Value {
    json!({
        "resourceLogs": [{
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": "checkout"}},
                    {"key": "deployment.environment", "value": {"stringValue": "prod"}},
                ],
            },
            "scopeLogs": [{
                "scope": {
                    "attributes": [
                        {"key": "instrumentation.scope", "value": {"stringValue": "api"}},
                    ],
                },
                "logRecords": [{
                    "timeUnixNano": timestamp_ns,
                    "body": {"stringValue": "checkout otlp loop error"},
                    "attributes": [
                        {"key": "status", "value": {"intValue": "500"}},
                        {"key": "trace_id", "value": {"stringValue": "abc123"}},
                    ],
                }],
            }],
        }],
    })
}

/// Issues `uri` until the querier answers it with a result.
///
/// Two things make one attempt too few. The querier's broker-backed authorizer
/// connects in the background and refuses every query until it lands, so a
/// non-OK status here means "not ready yet" rather than "wrong". And a fresh
/// consumer group takes a moment to be assigned its partition, so an empty
/// result means "not read yet". A querier that never answers still fails, with
/// its last status named.
async fn query_until_answered(querier: &Router, tenant: &str, uri: &str) -> Value {
    let deadline = Instant::now() + BROKER_DEADLINE;
    let mut last = StatusCode::OK;
    while Instant::now() < deadline {
        let response = querier
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("X-Scope-OrgID", tenant)
                    .body(Body::empty())
                    .expect("query request"),
            )
            .await
            .expect("query response");
        last = response.status();
        if last == StatusCode::OK {
            let body = json_body(response).await;
            if body
                .pointer("/data/result")
                .and_then(Value::as_array)
                .is_some_and(|result| !result.is_empty())
            {
                return body;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("the querier never answered {uri} with a result; last status {last}");
}

/// Polls `/ready` over TCP until the service reports ready.
///
/// A querier binds its HTTP port before its WAL consumer and its
/// broker-backed query authorizer connect, and it fails closed on every query
/// until both are up.
async fn wait_until_ready_at(addr: SocketAddr) {
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/ready");
    let deadline = Instant::now() + BROKER_DEADLINE;
    while Instant::now() < deadline {
        if client
            .get(&url)
            .send()
            .await
            .is_ok_and(|response| response.status() == reqwest::StatusCode::OK)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the served querier never reported ready at {addr}");
}

async fn poll_until_decoded(
    consumer: &mut KafkaLogWalConsumer,
    hot_tail: &BufferedLogHotTail,
) -> usize {
    let deadline = Instant::now() + BROKER_DEADLINE;
    let mut decoded = 0;
    while decoded == 0 && Instant::now() < deadline {
        decoded = poll_log_hot_tail_once(consumer, hot_tail, millis(250))
            .await
            .expect("poll the live wal");
    }
    decoded
}

async fn json_body(response: axum::response::Response) -> Value {
    let body = to_bytes(response.into_body(), 256 * 1024)
        .await
        .expect("read the response body");
    serde_json::from_slice(&body).expect("the response is json")
}

fn current_unix_second_ns() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the system clock is after the unix epoch")
        .as_secs();
    i64::try_from(now).expect("unix seconds fit in i64") * 1_000_000_000
}

fn fixture_timestamp_ns(offset_ns: i64) -> String {
    (current_unix_second_ns() + offset_ns).to_string()
}
