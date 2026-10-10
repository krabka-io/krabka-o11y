//! Fixtures and assertions that the observability HTTP suites share.

#![allow(dead_code)]

use std::collections::BTreeMap;

use assert2::{assert, check};
use async_trait::async_trait;
pub use axum::http::Method;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use datafusion::arrow::record_batch::RecordBatch;
use futures_util::StreamExt as _;
use krabka_blockstore::{
    BlockKey, LabelIndex, LogBlockIndex as BlockIndex, LogRow, TenantId, TimeRange, labels,
    write_log_block, write_log_block_to_object_store,
    write_tenant_log_index_shards_to_object_store,
};
use krabka_observability::{
    IngestLimitError, KafkaWalHeader, KafkaWalRecord, LogIngestLimiter, LogQueryAuthorizer,
    LogWalSink, Offset, PartitionIndex, QuerierIndexSource, QuerierState, QueryAuthorizationError,
    Role, ServiceConfig, WalLogRecord, WalSinkError, build_kafka_wal_record, build_querier_state,
    loki_router,
};
use krabka_units::convert::ByteSizeExt as _;
use object_store::{local::LocalFileSystem, path::Path as ObjectPath};
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tower::ServiceExt as _;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LokiProtoPushRequest {
    #[prost(message, repeated, tag = "1")]
    pub streams: Vec<LokiProtoStream>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LokiProtoStream {
    #[prost(string, tag = "1")]
    pub labels: String,
    #[prost(message, repeated, tag = "2")]
    pub entries: Vec<LokiProtoEntry>,
    #[prost(uint64, tag = "3")]
    pub hash: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LokiProtoEntry {
    #[prost(message, optional, tag = "1")]
    pub timestamp: Option<LokiProtoTimestamp>,
    #[prost(string, tag = "2")]
    pub line: String,
    #[prost(message, repeated, tag = "3")]
    pub structured_metadata: Vec<LokiProtoLabelPair>,
    #[prost(message, repeated, tag = "4")]
    pub parsed: Vec<LokiProtoLabelPair>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LokiProtoTimestamp {
    #[prost(int64, tag = "1")]
    pub seconds: i64,
    #[prost(int32, tag = "2")]
    pub nanos: i32,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LokiProtoLabelPair {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Clone)]
pub struct RejectingIngestLimiter;

#[async_trait]
impl LogIngestLimiter for RejectingIngestLimiter {
    async fn check(
        &self,
        _principal: &krabka_observability::server_security::Principal,
        tenant: &TenantId,
        records: &[WalLogRecord],
    ) -> Result<(), IngestLimitError> {
        assert!(tenant.as_str() == "tenant-a");
        assert!(records.len() == 1);
        Err(IngestLimitError::RateLimited {
            tenant: tenant.to_string(),
            reason: "tenant write quota exceeded".to_string(),
        })
    }
}

#[derive(Clone)]
pub struct DenyingQueryAuthorizer;

#[async_trait]
impl LogQueryAuthorizer for DenyingQueryAuthorizer {
    async fn check(
        &self,
        _principal: &krabka_observability::server_security::Principal,
        tenant: &TenantId,
    ) -> Result<(), QueryAuthorizationError> {
        Err(QueryAuthorizationError::Unauthorized {
            tenant: tenant.to_string(),
            reason: "tenant read ACL denied".to_string(),
        })
    }
}

/// Refuses one tenant and allows every other, so a test can show that a
/// refusal changes nothing for the refused tenant and nothing for the rest.
#[derive(Clone)]
pub struct TenantDenyingQueryAuthorizer {
    pub denied: &'static str,
}

#[async_trait]
impl LogQueryAuthorizer for TenantDenyingQueryAuthorizer {
    async fn check(
        &self,
        _principal: &krabka_observability::server_security::Principal,
        tenant: &TenantId,
    ) -> Result<(), QueryAuthorizationError> {
        if tenant.as_str() != self.denied {
            return Ok(());
        }
        Err(QueryAuthorizationError::Unauthorized {
            tenant: tenant.to_string(),
            reason: "tenant read ACL denied".to_string(),
        })
    }
}

#[derive(Clone)]
pub struct FailingWalSink;

#[async_trait]
impl LogWalSink for FailingWalSink {
    async fn append(&self, _record: WalLogRecord) -> Result<(), WalSinkError> {
        Err(WalSinkError::Append)
    }
}

/// A sink that appends `accept` records and then fails every further append.
///
/// It reproduces the partial push: the broker took some of one request's
/// entries and refused the rest.
pub struct PartialWalSink {
    accept: usize,
    appended: std::sync::Mutex<usize>,
}

impl PartialWalSink {
    #[must_use]
    pub fn new(accept: usize) -> Self {
        Self {
            accept,
            appended: std::sync::Mutex::new(0),
        }
    }
}

#[async_trait]
impl LogWalSink for PartialWalSink {
    async fn append(&self, _record: WalLogRecord) -> Result<(), WalSinkError> {
        let mut appended = self.appended.lock().expect("partial wal sink poisoned");
        if *appended >= self.accept {
            return Err(WalSinkError::Append);
        }
        *appended += 1;
        Ok(())
    }
}

pub fn fixture() -> QuerierState {
    fixture_with_time_scale(1)
}

pub fn loki_forwarded_fixture() -> QuerierState {
    fixture_with_time_scale(1_000_000_000)
}

fn fixture_with_time_scale(scale: i64) -> QuerierState {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let blocks = api_and_worker_blocks(&mut label_index, scale);
    label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "prod")]));

    let mut block_index = BlockIndex::default();
    for (key, rows) in blocks {
        block_index.insert(write_log_block(&dir, &key, rows).unwrap());
    }

    QuerierState::new(dir, label_index, block_index)
}

/// Adds tenant-a's `api` and `worker` series to `label_index`, and returns
/// the key and rows of an `api` block at 10-19 and a `worker` block at 20-29,
/// both in units of `scale` nanoseconds.
fn api_and_worker_blocks(label_index: &mut LabelIndex, scale: i64) -> [(BlockKey, Vec<LogRow>); 2] {
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    [
        (
            BlockKey::new(
                "tenant-a",
                0,
                10 * scale,
                19 * scale,
                TimeRange::new(10 * scale, 19 * scale).unwrap(),
            ),
            vec![
                LogRow::new(api, 10 * scale, "api ok", BTreeMap::new()),
                LogRow::new(api, 19 * scale, "api error", BTreeMap::new()),
            ],
        ),
        (
            BlockKey::new(
                "tenant-a",
                1,
                20 * scale,
                29 * scale,
                TimeRange::new(20 * scale, 29 * scale).unwrap(),
            ),
            vec![LogRow::new(
                worker,
                25 * scale,
                "worker error",
                BTreeMap::new(),
            )],
        ),
    ]
}

pub fn multi_tenant_fixture() -> (QuerierState, u64, u64) {
    multi_tenant_fixture_with_time_scale(1)
}

pub fn loki_forwarded_multi_tenant_fixture() -> (QuerierState, u64, u64) {
    multi_tenant_fixture_with_time_scale(1_000_000_000)
}

fn multi_tenant_fixture_with_time_scale(scale: i64) -> (QuerierState, u64, u64) {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let prod_api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let stage_api =
        label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "stage")]));

    let prod_block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            20 * scale,
            29 * scale,
            TimeRange::new(20 * scale, 29 * scale).unwrap(),
        ),
        vec![LogRow::new(
            prod_api,
            29 * scale,
            "tenant-a api error",
            BTreeMap::new(),
        )],
    )
    .unwrap();
    let stage_block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-b",
            0,
            20 * scale,
            29 * scale,
            TimeRange::new(20 * scale, 29 * scale).unwrap(),
        ),
        vec![LogRow::new(
            stage_api,
            29 * scale,
            "tenant-b api error",
            BTreeMap::new(),
        )],
    )
    .unwrap();
    let prod_bytes = prod_block.size.bytes_u64();
    let stage_bytes = stage_block.size.bytes_u64();

    let mut block_index = BlockIndex::default();
    block_index.insert(prod_block);
    block_index.insert(stage_block);

    (
        QuerierState::new(dir, label_index, block_index),
        prod_bytes,
        stage_bytes,
    )
}

pub async fn tenant_object_store_shard_catalog_config_fixture() -> (QuerierState, std::path::PathBuf)
{
    let (config, store, dir) =
        tenant_object_store_shard_catalog_service_fixture_with_time_scale(1_000_000_000).await;
    let state = build_querier_state(&config, Some(&store)).await.unwrap();

    (state, dir)
}

pub async fn tenant_object_store_shard_catalog_service_fixture()
-> (ServiceConfig, LocalFileSystem, std::path::PathBuf) {
    tenant_object_store_shard_catalog_service_fixture_with_time_scale(1).await
}

pub async fn loki_forwarded_tenant_object_store_shard_catalog_service_fixture()
-> (ServiceConfig, LocalFileSystem, std::path::PathBuf) {
    tenant_object_store_shard_catalog_service_fixture_with_time_scale(1_000_000_000).await
}

async fn tenant_object_store_shard_catalog_service_fixture_with_time_scale(
    scale: i64,
) -> (ServiceConfig, LocalFileSystem, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let mut block_index = BlockIndex::default();
    for (key, rows) in api_and_worker_blocks(&mut label_index, scale) {
        let block = write_log_block(&dir, &key, rows.clone()).unwrap();
        write_log_block_to_object_store(&store, &prefix, &block.key, rows)
            .await
            .unwrap();
        block_index.insert(block);
    }
    write_tenant_log_index_shards_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &[
            TimeRange::new(0, 19 * scale).unwrap(),
            TimeRange::new(20 * scale, 29 * scale).unwrap(),
        ],
        &label_index,
        &block_index,
    )
    .await
    .unwrap();

    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-block-builder".to_string(),
        data_root: dir.clone(),
        querier_index_source: QuerierIndexSource::TenantObjectStoreShards,
        tenant: Some("tenant-a".to_string()),
        index_prefix: Some(prefix.to_string()),
        query_start_ns: Some(0),
        query_end_ns: Some(19 * scale),
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_string_bytes: None,
        max_ingest_body: None,
        wal_append_timeout: None,
        ..ServiceConfig::default()
    };

    (config, store, dir)
}

pub fn proto_key_value(key: &str, value: any_value::Value) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue { value: Some(value) }),
        key_strindex: 0,
    }
}

pub fn proto_logs_request() -> ExportLogsServiceRequest {
    proto_logs_request_at_ns(19)
}

/// The same request, dated to `time_unix_nano`.
///
/// The ingest timestamp window refuses an entry older than the tenant's
/// `reject_old_samples_max_age`, so a test that goes through a configured
/// service has to date its entry inside that window.
pub fn proto_logs_request_at_ns(time_unix_nano: u64) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    proto_key_value(
                        "service.name",
                        any_value::Value::StringValue("checkout".into()),
                    ),
                    proto_key_value(
                        "deployment.environment",
                        any_value::Value::StringValue("prod".into()),
                    ),
                ],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "api".to_string(),
                    version: "1.2.3".to_string(),
                    attributes: vec![proto_key_value(
                        "instrumentation.scope",
                        any_value::Value::StringValue("api".into()),
                    )],
                    dropped_attributes_count: 0,
                }),
                log_records: vec![LogRecord {
                    time_unix_nano,
                    observed_time_unix_nano: 0,
                    severity_number: 0,
                    severity_text: String::new(),
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("api error".into())),
                    }),
                    attributes: vec![
                        proto_key_value("status", any_value::Value::IntValue(500)),
                        proto_key_value("trace_id", any_value::Value::StringValue("abc".into())),
                    ],
                    dropped_attributes_count: 0,
                    flags: 0,
                    trace_id: vec![],
                    span_id: vec![],
                    event_name: String::new(),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

pub async fn json_body(response: axum::response::Response) -> Value {
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    let mut value: Value = serde_json::from_slice(&body).unwrap();
    if let Some(summary) = value.pointer_mut("/data/stats/summary") {
        summary["bytesProcessedPerSecond"] = json!(0);
        summary["execTime"] = json!(0.0);
        summary["linesProcessedPerSecond"] = json!(0);
        summary["queueTime"] = json!(0.0);
    }
    value
}

/// A service config for `target` that names no object store, WAL broker,
/// tenant, or query limit.
pub fn minimal_service_config(target: Role) -> ServiceConfig {
    ServiceConfig {
        target,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-block-builder".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_string_bytes: None,
        max_ingest_body: None,
        wal_append_timeout: None,
        ..ServiceConfig::default()
    }
}

/// A POST to `uri` for tenant-a, ready for its headers and body.
pub fn tenant_a_post(uri: &str) -> axum::http::request::Builder {
    Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("X-Scope-OrgID", "tenant-a")
}

/// A form-encoded POST of `body` to `uri` for tenant-a.
pub async fn post_form(app: &Router, uri: &str, body: impl Into<Body>) -> axum::response::Response {
    send(
        app,
        tenant_a_post(uri)
            .header("content-type", "application/x-www-form-urlencoded")
            .body(body.into())
            .unwrap(),
    )
    .await
}

pub type TailSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Serves `app` on a loopback listener, and builds a tenant-a websocket
/// request for `path_and_query` against it.
pub async fn serve_for_websocket(
    app: Router,
    path_and_query: &str,
) -> (
    tokio_tungstenite::tungstenite::handshake::client::Request,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut request = format!("ws://{addr}{path_and_query}")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("X-Scope-OrgID", "tenant-a".parse().unwrap());
    (request, server)
}

/// Serves `app` and opens a tenant-a tail websocket at `path_and_query`.
pub async fn open_tail(
    app: Router,
    path_and_query: &str,
) -> (TailSocket, tokio::task::JoinHandle<()>) {
    let (request, server) = serve_for_websocket(app, path_and_query).await;
    let (socket, response) = tokio_tungstenite::connect_async(request).await.unwrap();
    assert!(response.status() == StatusCode::SWITCHING_PROTOCOLS);
    (socket, server)
}

/// The next text frame on `socket`, parsed as JSON.
pub async fn next_frame(socket: &mut TailSocket) -> Value {
    let message = socket.next().await.unwrap().unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}

/// The next text frame on `socket`, parsed as JSON, which must arrive within
/// two seconds.
pub async fn next_frame_within_two_seconds(socket: &mut TailSocket) -> Value {
    let message = tokio::time::timeout(std::time::Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}

/// The tenant a request names in `X-Scope-OrgID`.
#[derive(Clone, Copy)]
pub struct Tenant<'a>(pub &'a str);

impl Tenant<'_> {
    /// A bodiless request with `method` to `uri` for this tenant.
    pub fn request(self, method: Method, uri: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("X-Scope-OrgID", self.0)
            .body(Body::empty())
            .unwrap()
    }

    /// Sends this tenant's bodiless `method` request to `uri`.
    pub async fn send(self, app: &Router, method: Method, uri: &str) -> axum::response::Response {
        send(app, self.request(method, uri)).await
    }

    /// Sends this tenant's GET of `uri`.
    pub async fn get(self, app: &Router, uri: &str) -> axum::response::Response {
        self.send(app, Method::GET, uri).await
    }

    /// Sends this tenant's GET of `uri`, and reads back its status and JSON.
    pub async fn get_json(self, app: &Router, uri: &str) -> (StatusCode, Value) {
        let response = self.get(app, uri).await;
        (response.status(), json_body(response).await)
    }
}

pub async fn send(app: &Router, request: Request<Body>) -> axum::response::Response {
    app.clone().oneshot(request).await.unwrap()
}

/// A bodiless request with `method` to `uri` that names no tenant.
pub async fn send_bare(app: &Router, method: Method, uri: &str) -> axum::response::Response {
    send(
        app,
        Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await
}

/// Checks that `response` is a 200 whose JSON body, with its timing stats
/// zeroed, is `expected`.
pub async fn assert_json_ok(response: axum::response::Response, expected: &Value) {
    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == *expected);
}

pub async fn text_body(response: axum::response::Response) -> String {
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    String::from_utf8(body.to_vec()).unwrap()
}

pub fn current_unix_epoch_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_nanos()
}

pub fn test_service_config(
    target: Role,
    data_root: impl Into<std::path::PathBuf>,
) -> ServiceConfig {
    let index_prefix = if matches!(target, Role::BlockBuilder) {
        Some("observability/logs".to_string())
    } else {
        None
    };
    ServiceConfig {
        target,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-test".to_string(),
        data_root: data_root.into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_string_bytes: None,
        max_ingest_body: None,
        wal_append_timeout: None,
        ..ServiceConfig::default()
    }
}

pub fn assert_loki_error(body: &Value, error_type: &str, error_contains: &str) {
    assert!(body["status"] == "error");
    assert!(body["errorType"] == error_type);
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains(error_contains))
    );
    assert!(body["data"].is_null());
}

pub fn expected_api_error() -> Value {
    expected_api_error_with_stats(&expected_loki_stats_with(1846, 1, 1))
}

pub fn expected_loki_forwarded_api_error() -> Value {
    expected_loki_forwarded_api_error_with_stats(&expected_loki_stats_with(1846, 1, 1))
}

pub fn expected_api_error_with_stats(stats: &Value) -> Value {
    expected_api_error_at_with_stats("19", stats)
}

pub fn expected_loki_forwarded_api_error_with_stats(stats: &Value) -> Value {
    expected_api_error_at_with_stats("19000000000", stats)
}

fn expected_api_error_at_with_stats(timestamp_ns: &str, stats: &Value) -> Value {
    json!({
        "status": "success",
        "data": {
            "resultType": "streams",
            "result": [
                {
                    "stream": {
                        "app": "api",
                        "env": "prod"
                    },
                    "values": [
                        [timestamp_ns, "api error"]
                    ]
                }
            ],
            "stats": stats
        }
    })
}

/// A tenant-a WAL record of the `{app="api", env="prod"}` series, with no
/// structured metadata and no WAL position.
pub fn api_prod_wal_record(timestamp_ns: i64, line: &str) -> WalLogRecord {
    WalLogRecord {
        tenant: "tenant-a".to_string(),
        labels: labels([("app", "api"), ("env", "prod")]),
        timestamp_ns,
        line: line.to_string(),
        structured_metadata: BTreeMap::new(),
        position: None,
    }
}

/// A streams result that holds one `{app="api", env="prod"}` stream.
pub fn api_prod_streams(values: Value) -> Value {
    let mut stream = json!({ "stream": { "app": "api", "env": "prod" } });
    stream["values"] = values;
    Value::Array(vec![stream])
}

/// A querier over one tenant-a block that holds `api ok` at 10 s and
/// `api error` at 19 s. Returns the router and the block's size in bytes.
pub fn api_seconds_block_app() -> (Router, u64) {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            10_000_000_000,
            19_000_000_000,
            TimeRange::new(10_000_000_000, 19_000_000_000).unwrap(),
        ),
        vec![
            LogRow::new(api, 10_000_000_000, "api ok", BTreeMap::new()),
            LogRow::new(api, 19_000_000_000, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();
    let bytes = api_block.size.bytes_u64();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    (
        loki_router(QuerierState::new(dir, label_index, block_index)),
        bytes,
    )
}

/// Checks the stats of a query that read one line from one stored block of
/// `block_bytes`.
pub fn check_one_stored_line_stats(body: &Value, block_bytes: u64) {
    let stats = &body["data"]["stats"];
    check!(stats["store"]["compressedBytes"] == block_bytes);
    check!(stats["store"]["decompressedBytes"] == block_bytes);
    check!(stats["store"]["decompressedLines"] == 1);
    check!(stats["store"]["totalChunksRef"] == 1);
    check!(stats["store"]["totalChunksDownloaded"] == 1);
    check!(stats["summary"]["totalBytesProcessed"] == block_bytes);
    check!(stats["summary"]["totalLinesProcessed"] == 1);
}

/// Requests `uri` for tenant-a as Parquet, checks the response says so, and
/// returns its one record batch.
pub async fn parquet_batch(app: Router, uri: &str) -> RecordBatch {
    let response = send(
        &app,
        Request::builder()
            .uri(uri)
            .header("X-Scope-OrgID", "tenant-a")
            .header("accept", "application/vnd.apache.parquet")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert!(response.status() == StatusCode::OK);
    assert!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            == Some("application/vnd.apache.parquet")
    );
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    let mut reader = ParquetRecordBatchReader::try_new(body, 1024).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());
    batch
}

/// A Loki success envelope.
pub struct LokiSuccess<'a> {
    /// `streams`, `matrix`, `vector` or `scalar`.
    pub result_type: &'a str,
    /// The `data.result` member.
    pub data_result: Value,
    pub stats: Value,
}

impl LokiSuccess<'_> {
    /// The envelope as the JSON body Loki sends.
    pub fn json(self) -> Value {
        let mut body = json!({
            "status": "success",
            "data": {
                "resultType": self.result_type
            }
        });
        body["data"]["result"] = self.data_result;
        body["data"]["stats"] = self.stats;
        body
    }
}

pub fn expected_loki_stats() -> Value {
    expected_loki_stats_with(0, 0, 0)
}

pub fn expected_loki_stats_with(bytes: u64, lines: u64, chunks: u64) -> Value {
    expected_loki_mixed_stats_with(bytes, lines, 0, chunks)
}

/// The stats of a query that only the ingester's hot tail answered.
pub fn expected_loki_ingester_stats_with(lines: u64) -> Value {
    expected_loki_mixed_stats_with(0, 0, lines, 0)
}

/// The Kafka record the WAL producer writes for `record`, as a consumer reads
/// it back at `partition` and `offset`.
pub fn kafka_wal_record(
    record: &WalLogRecord,
    partition: PartitionIndex,
    offset: Offset,
) -> KafkaWalRecord {
    let producer_record =
        build_kafka_wal_record("__krabka_observability_logs_wal", record).expect("producer record");
    KafkaWalRecord {
        value: producer_record.value.expect("producer value").to_vec(),
        partition,
        offset,
        timestamp_ms: producer_record.timestamp_ms,
        headers: producer_record
            .headers
            .into_iter()
            .map(|header| KafkaWalHeader {
                key: header.key,
                value: header.value.map(|value| value.to_vec()),
            })
            .collect(),
    }
}

pub fn expected_loki_mixed_stats_with(
    bytes: u64,
    store_lines: u64,
    ingester_lines: u64,
    chunks: u64,
) -> Value {
    json!({
        "ingester": {
            "compressedBytes": 0,
            "decompressedBytes": 0,
            "decompressedLines": ingester_lines,
            "headChunkBytes": 0,
            "headChunkLines": 0,
            "totalBatches": 0,
            "totalChunksMatched": 0,
            "totalDuplicates": 0,
            "totalLinesSent": ingester_lines,
            "totalReached": 0
        },
        "store": {
            "compressedBytes": bytes,
            "decompressedBytes": bytes,
            "decompressedLines": store_lines,
            "chunksDownloadTime": 0.0,
            "totalChunksRef": chunks,
            "totalChunksDownloaded": chunks,
            "totalDuplicates": 0
        },
        "summary": {
            "bytesProcessedPerSecond": 0,
            "execTime": 0.0,
            "linesProcessedPerSecond": 0,
            "queueTime": 0.0,
            "totalBytesProcessed": bytes,
            "totalLinesProcessed": store_lines + ingester_lines
        }
    })
}

/// A block's first and last WAL offset. The fixtures give each block the same
/// span in nanoseconds, so it is the block's time range too.
#[derive(Clone, Copy)]
pub struct BlockSpan {
    pub first: i64,
    pub last: i64,
}

/// One log line and its timestamp in nanoseconds.
#[derive(Clone, Copy)]
pub struct LogEntry<'a> {
    pub timestamp_ns: i64,
    pub line: &'a str,
}

pub const fn log_entry(timestamp_ns: i64, line: &str) -> LogEntry<'_> {
    LogEntry { timestamp_ns, line }
}

/// What one push did: the response status and body, and the records the
/// WAL sink holds afterwards.
pub struct PushOutcome {
    pub status: StatusCode,
    pub body: String,
    pub records: Vec<WalLogRecord>,
}

impl PushOutcome {
    pub fn accepted(&self) -> &[WalLogRecord] {
        assert!(self.status == StatusCode::NO_CONTENT);
        &self.records
    }

    pub fn accepted_without_records(&self) {
        check!(self.status == StatusCode::NO_CONTENT);
        check!(self.body.is_empty());
        check!(self.records.is_empty());
    }

    pub fn rejected_with(&self, status: StatusCode, body: &str) {
        check!(self.status == status);
        check!(self.body == body);
        check!(self.records.is_empty());
    }

    pub fn rejected_containing(&self, fragments: &[&str]) {
        assert!(self.status == StatusCode::BAD_REQUEST);
        for fragment in fragments {
            check!(self.body.contains(fragment));
        }
        check!(self.records.is_empty());
    }

    pub fn rejected_as_loki_error(&self, expected: &ExpectedLokiError) {
        assert!(self.status == expected.status);
        assert_loki_error(
            &serde_json::from_str(&self.body).unwrap(),
            expected.error_type,
            expected.contains,
        );
        assert!(self.records.is_empty());
    }
}

/// The Loki error a rejected push answers with.
pub struct ExpectedLokiError<'a> {
    pub status: StatusCode,
    pub error_type: &'a str,
    /// A fragment the error message holds.
    pub contains: &'a str,
}

/// The headers that describe a push body.
#[derive(Clone, Copy)]
pub struct BodyHeaders<'a> {
    pub content_type: &'a str,
    pub content_encoding: Option<&'a str>,
}

/// A tenant-a push of `body` to `uri`, described by `headers`.
pub fn push_request(uri: &str, headers: BodyHeaders, body: impl Into<Body>) -> Request<Body> {
    let mut builder = tenant_a_post(uri).header("content-type", headers.content_type);
    if let Some(encoding) = headers.content_encoding {
        builder = builder.header("content-encoding", encoding);
    }
    builder.body(body.into()).unwrap()
}

/// One stream of a JSON push.
pub struct JsonStream {
    pub stream: Value,
    pub values: Value,
}

impl JsonStream {
    /// The push body that carries only this stream.
    pub fn payload(self) -> Value {
        let mut stream = serde_json::Map::new();
        stream.insert("stream".to_string(), self.stream);
        stream.insert("values".to_string(), self.values);
        json!({ "streams": [Value::Object(stream)] })
    }
}
