//! End-to-end profiles ingest, from the `push.v1` door to the distributor,
//! then to the broker WAL, then to the block-builder, then to an object-store
//! block, and finally back out through the querier's render door.
//!
//! Every other profiles suite either stops at a capturing sink or needs a
//! Pyroscope container. This one boots a real broker in process, so the WAL
//! record it asserts on is the one the distributor actually produced and the
//! block it queries is the one the block-builder actually wrote.

use std::{
    collections::BTreeMap,
    io::Write as _,
    sync::Arc,
    time::{Duration, Instant},
};

use assert2::{assert, check};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use flate2::{Compression, write::GzEncoder};
use krabka_blockstore::ProfileIndex;
use krabka_broker::{Broker, BrokerConfig};
use krabka_client_admin::{AdminClient, CreateTopicSpec};
use krabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerRecord};
use krabka_client_producer::Producer;
use krabka_pprof::{PprofProfile, proto};
use krabka_profiles::{
    PROFILES_WAL_TOPIC, ProfileRecord, WalSample,
    blockbuilder::{DEFAULT_FLUSH_RECORDS, flush_consumer_records_with_index},
    cold_store::ColdProfileStore,
    distributor::{self, DistributorState, KafkaSink},
    ingest::TenantLimitConfig,
    limits::{Limits, OverridesProvider},
    metrics::ServiceMetrics,
    query::{self, QuerierState},
};
use krabka_units::convert::TimeExt as _;
use object_store::{ObjectStore, ObjectStoreExt as _, memory::InMemory};
use serde_json::{Value, json};
use tower::ServiceExt as _;

const TENANT: &str = "tenant-a";
const PROFILE_NAME: &str = "process_cpu";
const PROFILE_TYPE: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const SERVICE: &str = "checkout";
const PROFILE_ID: &str = "krabka-ingest-roundtrip";
const SELECTOR: &str = r#"{service_name="checkout"}"#;
const FUNC_WORK: &str = "main.work";
const FUNC_HOT: &str = "main.hotloop";
/// `main.hotloop` under `main.work`, plus `main.work` on its own.
const LEAF_VALUE: i64 = 100;
const SELF_VALUE: i64 = 40;
/// Whole seconds, so the block-builder's nanosecond-to-millisecond truncation
/// cannot make the expected timestamp depend on rounding.
const PROFILE_TIME_NANOS: i64 = 1_700_000_000_000_000_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pushed_profile_lands_in_a_queryable_block() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(tempdir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    create_profiles_wal_topic(&bootstrap).await;

    let producer = Producer::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .expect("producer build");
    let state = distributor_state(Arc::new(KafkaSink::new(Arc::new(producer))));
    let response = distributor::router(state)
        .oneshot(push_request())
        .await
        .expect("push response");
    assert!(response.status() == StatusCode::OK);

    let wal_records = consume_wal_records(&bootstrap).await;
    assert!(wal_records.len() == 1);
    let wal_record = decode_wal_record(&wal_records[0]);

    check!(wal_record.tenant == TENANT);
    check!(wal_record.profile_type == PROFILE_TYPE);
    // The pushed label set plus the meta labels the distributor derives from
    // the pprof header: the profile type and its parts, the sample id, and the
    // normalized service name.
    check!(
        wal_record.labels
            == vec![
                ("__name__".to_string(), PROFILE_NAME.to_string()),
                ("__period_type__".to_string(), "cpu".to_string()),
                ("__period_unit__".to_string(), "nanoseconds".to_string()),
                ("__profile_id__".to_string(), PROFILE_ID.to_string()),
                ("__profile_type__".to_string(), PROFILE_TYPE.to_string()),
                ("__service_name__".to_string(), SERVICE.to_string()),
                ("__type__".to_string(), "cpu".to_string()),
                ("__unit__".to_string(), "nanoseconds".to_string()),
                ("service_name".to_string(), SERVICE.to_string()),
            ]
    );
    // Leaf-first location refs, one per pprof sample, and the values the
    // profile carried. `stacktrace_location_refs` indexes `symbols.locations`.
    check!(
        wal_record.samples
            == vec![
                WalSample {
                    stacktrace_location_refs: vec![1, 0],
                    value: LEAF_VALUE,
                    timestamp_ns: PROFILE_TIME_NANOS,
                    span_id: None,
                    trace_id: None,
                },
                WalSample {
                    stacktrace_location_refs: vec![0],
                    value: SELF_VALUE,
                    timestamp_ns: PROFILE_TIME_NANOS,
                    span_id: None,
                    trace_id: None,
                },
            ]
    );
    // The symbols travel with the record: the block-builder has no other source
    // for them, so a WAL record that lost them would build a nameless block.
    check!(
        function_names(&wal_record) == vec![FUNC_WORK.to_string(), FUNC_HOT.to_string()],
        "WAL record must carry the profile's function names"
    );

    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut index = ProfileIndex::new();
    let metas = flush_consumer_records_with_index(
        &object_store,
        &mut index,
        &wal_records,
        DEFAULT_FLUSH_RECORDS,
        &krabka_blockstore::ObjectStoreMetrics::unregistered(),
    )
    .await
    .expect("flush wal records into a block");

    assert!(metas.len() == 1);
    let meta = &metas[0];
    check!(meta.tenant == TENANT);
    check!(meta.row_count == 2);
    check!(meta.min_ts == PROFILE_TIME_NANOS / 1_000_000);
    check!(meta.max_ts == PROFILE_TIME_NANOS / 1_000_000);
    check!(meta.fingerprints == vec![wal_record.series_fingerprint()]);
    // The block and its symbol database are both in the object store, under the
    // key the meta advertises.
    for key in [
        meta.object_key.clone(),
        format!("{}.symdb", meta.object_key),
    ] {
        let head = object_store
            .head(&object_store::path::Path::from(key.clone()))
            .await;
        check!(head.is_ok(), "block-builder must have written {key}");
    }

    let render = render_flamebearer(object_store, index).await;
    check!(flame_names(&render) == vec![FUNC_HOT.to_string(), FUNC_WORK.to_string()]);
    check!(flame_ticks(&render) == Some(LEAF_VALUE + SELF_VALUE));
    check!(
        render.pointer("/metadata/units").and_then(Value::as_str) == Some("nanoseconds"),
        "render metadata must carry the profile type's unit, got {render}"
    );
}

/// A push whose `X-Scope-OrgID` is path-unsafe is rejected at the door, and
/// nothing reaches the WAL. Run against the same broker-backed sink as the
/// happy path, so an empty topic here is the door's doing and not the sink's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_push_with_a_path_unsafe_tenant_never_reaches_the_wal() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(tempdir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    create_profiles_wal_topic(&bootstrap).await;

    let producer = Producer::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .expect("producer build");
    let state = distributor_state(Arc::new(KafkaSink::new(Arc::new(producer))));
    let request = Request::builder()
        .method("POST")
        .uri("/push.v1.PusherService/Push")
        .header("Content-Type", "application/json")
        .header("x-scope-orgid", "../escape")
        .body(Body::from(
            serde_json::to_vec(&push_body()).expect("serialize push body"),
        ))
        .expect("request");
    let response = distributor::router(state)
        .oneshot(request)
        .await
        .expect("push response");

    check!(response.status() == StatusCode::BAD_REQUEST);
    assert!(consume_wal_records(&bootstrap).await.is_empty());
}

fn distributor_state(sink: Arc<KafkaSink>) -> Arc<DistributorState> {
    Arc::new(DistributorState {
        sink,
        limits: TenantLimitConfig::default(),
        profile_overrides: OverridesProvider::new(Limits::default()),
        active_series: std::sync::Mutex::default(),
        ingestion_buckets: std::sync::Mutex::default(),
        relabel: Vec::new(),
        max_decompressed: krabka_units::mebibytes(16),
        max_tracked_tenants: 4096,
        legacy_decode_limits: krabka_profiles::ingest::LegacyDecodeLimits::default(),
        metrics: ServiceMetrics::new(),
    })
}

async fn create_profiles_wal_topic(bootstrap: &str) {
    let mut admin = AdminClient::connect(&[bootstrap.to_string()])
        .await
        .expect("admin connect");
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: PROFILES_WAL_TOPIC.into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::default(),
            }],
            krabka_units::secs(5),
        )
        .await
        .expect("create profiles wal topic");
}

/// Every WAL record the topic holds, polled until one arrives or the deadline
/// passes. An empty answer therefore means the deadline elapsed with the topic
/// empty, which is what the negative test asserts.
async fn consume_wal_records(bootstrap: &str) -> Vec<ConsumerRecord> {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("profiles-roundtrip-inspect")
        .client_id("profiles-roundtrip-inspect")
        .subscribe([PROFILES_WAL_TOPIC.to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("inspect consumer");
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut out = Vec::new();
    while Instant::now() < deadline && out.is_empty() {
        let records = consumer
            .poll(krabka_units::millis(250))
            .await
            .expect("poll inspect consumer");
        out.extend(
            records
                .into_iter()
                .filter(|record| record.topic == PROFILES_WAL_TOPIC),
        );
    }
    out
}

fn decode_wal_record(record: &ConsumerRecord) -> ProfileRecord {
    check!(record.partition == 0);
    check!(record.offset == 0);
    let value = record.value.as_ref().expect("wal record value");
    ProfileRecord::decode(value).expect("decode wal record")
}

/// The record's function names in `symbols.functions` order, resolved through
/// its string table.
fn function_names(record: &ProfileRecord) -> Vec<String> {
    record
        .symbols
        .functions
        .iter()
        .map(|function| {
            record
                .symbols
                .strings
                .get(function.name as usize)
                .cloned()
                .unwrap_or_default()
        })
        .collect()
}

/// Query the block the block-builder wrote, through the querier's real render
/// door, and return the flamebearer JSON.
async fn render_flamebearer(object_store: Arc<dyn ObjectStore>, index: ProfileIndex) -> Value {
    let cold = ColdProfileStore::new(object_store, Arc::new(index));
    // The default per-query range cap would reject the unbounded window this
    // test uses to prove the sample is findable without knowing its timestamp.
    let querier = Arc::new(QuerierState::new_with_limits(
        Arc::new(cold),
        Limits {
            max_query_length: krabka_units::Time::ZERO,
            ..Default::default()
        },
    ));
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("query", &format!("{PROFILE_TYPE}{SELECTOR}"))
        .append_pair("from", "0")
        .append_pair("until", &i64::MAX.to_string())
        .finish();
    let request = Request::builder()
        .method("GET")
        .uri(format!("/pyroscope/render?{query}"))
        .header("x-scope-orgid", TENANT)
        .body(Body::empty())
        .expect("render request");
    let response = query::router(querier)
        .oneshot(request)
        .await
        .expect("render response");
    assert!(response.status() == StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("render body");
    serde_json::from_slice(&body).expect("render json")
}

fn flame_names(value: &Value) -> Vec<String> {
    let mut names: Vec<String> = value
        .pointer("/flamebearer/names")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|names| names.iter())
        .filter_map(Value::as_str)
        .filter(|name| *name != "total" && !name.is_empty())
        .map(ToString::to_string)
        .collect();
    names.sort();
    names
}

fn flame_ticks(value: &Value) -> Option<i64> {
    value
        .pointer("/flamebearer/numTicks")
        .or_else(|| value.pointer("/flamebearer/total"))
        .and_then(Value::as_i64)
}

fn push_request() -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/push.v1.PusherService/Push")
        .header("Content-Type", "application/json")
        .header("x-scope-orgid", TENANT)
        .body(Body::from(
            serde_json::to_vec(&push_body()).expect("serialize push body"),
        ))
        .expect("request")
}

fn push_body() -> Value {
    json!({
        "series": [{
            "labels": [
                { "name": "__name__", "value": PROFILE_NAME },
                { "name": "service_name", "value": SERVICE }
            ],
            "samples": [{
                "rawProfile": BASE64.encode(gzip_bytes(&synthetic_cpu_pprof())),
                "ID": PROFILE_ID
            }]
        }]
    })
}

/// A two-sample CPU profile: `main.hotloop` called from `main.work`, and
/// `main.work` on its own.
fn synthetic_cpu_pprof() -> Vec<u8> {
    // string_table: 0="" 1="cpu" 2="nanoseconds" 3=main.work 4=main.hotloop 5="app.go"
    let profile = proto::Profile {
        sample_type: vec![proto::ValueType { r#type: 1, unit: 2 }],
        sample: vec![
            proto::Sample {
                location_id: vec![2, 1], // leaf-first: main.hotloop -> main.work
                value: vec![LEAF_VALUE],
                label: Vec::new(),
            },
            proto::Sample {
                location_id: vec![1], // main.work
                value: vec![SELF_VALUE],
                label: Vec::new(),
            },
        ],
        mapping: vec![proto::Mapping {
            id: 1,
            symbolization: proto::MappingSymbolization::from_parts((true, false, false, false)),
            ..Default::default()
        }],
        location: vec![
            proto::Location {
                id: 1,
                mapping_id: 1,
                address: 0x1000,
                line: vec![proto::Line {
                    function_id: 1,
                    line: 10,
                    column: 0,
                }],
                is_folded: false,
            },
            proto::Location {
                id: 2,
                mapping_id: 1,
                address: 0x2000,
                line: vec![proto::Line {
                    function_id: 2,
                    line: 20,
                    column: 0,
                }],
                is_folded: false,
            },
        ],
        function: vec![
            proto::Function {
                id: 1,
                name: 3,
                system_name: 3,
                filename: 5,
                start_line: 1,
            },
            proto::Function {
                id: 2,
                name: 4,
                system_name: 4,
                filename: 5,
                start_line: 2,
            },
        ],
        string_table: vec![
            String::new(),
            "cpu".to_string(),
            "nanoseconds".to_string(),
            FUNC_WORK.to_string(),
            FUNC_HOT.to_string(),
            "app.go".to_string(),
        ],
        time_nanos: PROFILE_TIME_NANOS,
        duration_nanos: 1_000_000_000,
        period_type: Some(proto::ValueType { r#type: 1, unit: 2 }),
        period: 10_000_000,
        ..Default::default()
    };
    PprofProfile::from(profile).encode()
}

fn gzip_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes).expect("gzip write");
    encoder.finish().expect("gzip finish")
}
