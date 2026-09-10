//! Fixtures and assertions that the observability HTTP suites share.

#![allow(dead_code)]

use std::collections::BTreeMap;

use assert2::assert;
use async_trait::async_trait;
use axum::body::to_bytes;
use krabka_blockstore::{
    BlockKey, LabelIndex, LogBlockIndex as BlockIndex, LogRow, TimeRange, labels, write_log_block,
    write_log_block_to_object_store, write_tenant_log_index_shards_to_object_store,
};
use krabka_observability::{
    IngestLimitError, LogIngestLimiter, LogQueryAuthorizer, LogWalSink, QuerierIndexSource,
    QuerierState, QueryAuthorizationError, Role, ServiceConfig, WalLogRecord, WalSinkError,
    build_querier_state,
};
use krabka_units::convert::ByteSizeExt as _;
use object_store::{local::LocalFileSystem, path::Path as ObjectPath};
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use serde_json::{Value, json};

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
    async fn check(&self, tenant: &str, records: &[WalLogRecord]) -> Result<(), IngestLimitError> {
        assert!(tenant == "tenant-a");
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
    async fn check(&self, tenant: &str) -> Result<(), QueryAuthorizationError> {
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

pub fn fixture() -> QuerierState {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "prod")]));

    let mut block_index = BlockIndex::default();
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();
    let worker_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 1, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(worker, 25, "worker error", BTreeMap::new())],
    )
    .unwrap();
    block_index.insert(api_block);
    block_index.insert(worker_block);

    QuerierState::new(dir, label_index, block_index)
}

pub fn multi_tenant_fixture() -> (QuerierState, u64, u64) {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let prod_api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let stage_api =
        label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "stage")]));

    let prod_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(
            prod_api,
            29,
            "tenant-a api error",
            BTreeMap::new(),
        )],
    )
    .unwrap();
    let stage_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-b", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(
            stage_api,
            29,
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
    let (config, store, dir) = tenant_object_store_shard_catalog_service_fixture().await;
    let state = build_querier_state(&config, Some(&store)).await.unwrap();

    (state, dir)
}

pub async fn tenant_object_store_shard_catalog_service_fixture()
-> (ServiceConfig, LocalFileSystem, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));

    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();
    write_log_block_to_object_store(
        &store,
        &prefix,
        &api_block.key,
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .await
    .unwrap();
    let worker_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 1, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(worker, 25, "worker error", BTreeMap::new())],
    )
    .unwrap();
    write_log_block_to_object_store(
        &store,
        &prefix,
        &worker_block.key,
        vec![LogRow::new(worker, 25, "worker error", BTreeMap::new())],
    )
    .await
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    block_index.insert(worker_block);
    write_tenant_log_index_shards_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &[
            TimeRange::new(0, 19).unwrap(),
            TimeRange::new(20, 29).unwrap(),
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
        wal_group_id: "krabka-observability-compactor".to_string(),
        data_root: dir.clone(),
        querier_index_source: QuerierIndexSource::TenantObjectStoreShards,
        tenant: Some("tenant-a".to_string()),
        index_prefix: Some(prefix.to_string()),
        query_start_ns: Some(0),
        query_end_ns: Some(19),
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_length: None,
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
                    time_unix_nano: 19,
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
    serde_json::from_slice(&body).unwrap()
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
    let index_prefix = if matches!(target, Role::Compactor) {
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
        max_query_length: None,
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
    expected_api_error_with_stats(&expected_loki_stats_with(1819, 1, 1))
}

pub fn expected_api_error_with_stats(stats: &Value) -> Value {
    json!({
        "status": "success",
        "data": {
            "resultType": "streams",
            "result": [
                {
                    "stream": {
                        "app": "api",
                        "detected_level": "unknown",
                        "env": "prod"
                    },
                    "values": [
                        ["19", "api error"]
                    ]
                }
            ],
            "stats": stats
        }
    })
}

pub fn expected_loki_stats() -> Value {
    expected_loki_stats_with(0, 0, 0)
}

pub fn expected_loki_stats_with(bytes: u64, lines: u64, chunks: u64) -> Value {
    json!({
        "ingester": {
            "compressedBytes": 0,
            "decompressedBytes": 0,
            "decompressedLines": 0,
            "headChunkBytes": 0,
            "headChunkLines": 0,
            "totalBatches": 0,
            "totalChunksMatched": 0,
            "totalDuplicates": 0,
            "totalLinesSent": 0,
            "totalReached": 0
        },
        "store": {
            "compressedBytes": bytes,
            "decompressedBytes": bytes,
            "decompressedLines": lines,
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
            "totalLinesProcessed": lines
        }
    })
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
