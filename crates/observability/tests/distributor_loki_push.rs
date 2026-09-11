//! The Loki push endpoint, and the WAL records it writes.

mod support;

use std::{collections::BTreeMap, io::Write as _};

use assert2::{assert, check};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use flate2::{
    Compression,
    write::{DeflateEncoder, GzEncoder},
};
use krabka_blockstore::labels;
use krabka_observability::{
    InMemoryWalSink, QuerierIndexSource, Role, ServiceConfig, ServiceDependencies, WalLogRecord,
    build_service_router, distributor_router,
};
use prost::Message as _;
use serde_json::json;
use snap::raw::Encoder as SnappyEncoder;
use support::{
    FailingWalSink, LokiProtoEntry, LokiProtoLabelPair, LokiProtoPushRequest, LokiProtoStream,
    LokiProtoTimestamp, PartialWalSink, assert_loki_error, json_body, text_body,
};
use tower::ServiceExt as _;

#[tokio::test]
async fn loki_push_endpoint_writes_tenant_scoped_wal_records() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api",
                                    "env": "prod"
                                },
                                "values": [
                                    ["19", "api error", {"trace_id": "abc", "status": "500"}],
                                    ["20", "api ok"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(
        records
            == vec![
                WalLogRecord {
                    tenant: "tenant-a".to_string(),
                    labels: labels([
                        ("app", "api"),
                        ("detected_level", "error"),
                        ("env", "prod"),
                        ("service_name", "api"),
                    ]),
                    timestamp_ns: 19,
                    line: "api error".to_string(),
                    structured_metadata: BTreeMap::from([
                        ("status".to_string(), "500".to_string()),
                        ("trace_id".to_string(), "abc".to_string()),
                    ]),
                    position: None,
                },
                WalLogRecord {
                    tenant: "tenant-a".to_string(),
                    labels: labels([("app", "api"), ("env", "prod"), ("service_name", "api")]),
                    timestamp_ns: 20,
                    line: "api ok".to_string(),
                    structured_metadata: BTreeMap::new(),
                    position: None,
                },
            ]
    );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_incomplete_json_value_as_empty_line() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api",
                                    "env": "prod"
                                },
                                "values": [
                                    ["19"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(
        records
            == vec![WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([("app", "api"), ("env", "prod"), ("service_name", "api")]),
                timestamp_ns: 19,
                line: String::new(),
                structured_metadata: BTreeMap::new(),
                position: None,
            }]
    );
}

#[tokio::test]
async fn loki_push_endpoint_ignores_extra_json_value_fields_like_loki() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["19", "api error", {"trace_id": "abc"}, "extra"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(
        records
            == vec![WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([
                    ("app", "api"),
                    ("detected_level", "error"),
                    ("service_name", "api"),
                ]),
                timestamp_ns: 19,
                line: "api error".to_string(),
                structured_metadata: BTreeMap::from([("trace_id".to_string(), "abc".to_string())]),
                position: None,
            }]
    );
}

#[tokio::test]
async fn loki_push_endpoint_decodes_empty_json_value_as_zero_timestamp_empty_line_like_loki() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    []
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(
        records
            == vec![WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([("app", "api"), ("service_name", "api")]),
                timestamp_ns: 0,
                line: String::new(),
                structured_metadata: BTreeMap::new(),
                position: None,
            }]
    );
}

#[tokio::test]
async fn loki_push_endpoint_rejects_non_array_json_value_like_loki() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["19", "api ok"],
                                    "not-a-push-value"
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = text_body(response).await;
    check!(body.contains(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Unknown value type"
    ));
    check!(body.contains("not-a-push-value"));
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_non_object_json_stream_like_loki() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            "not-a-stream"
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = text_body(response).await;
    check!(body.contains(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like object"
    ));
    check!(body.contains("not-a-stream"));
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_non_array_json_streams_like_loki() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": "not-streams"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = text_body(response).await;
    check!(body.contains(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: decode slice: expect [ or n, but found"
    ));
    check!(body.contains("not-streams"));
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_array_json_payload_like_loki() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(r#"[{"streams": []}]"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = text_body(response).await;
    check!(body.contains("readObjectStart: expect { or n, but found ["));
    check!(body.contains(r#"[{"streams""#));
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_null_json_payload_like_loki() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from("null"))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::UNPROCESSABLE_ENTITY);
    check!(
        text_body(response).await == "error at least one valid stream is required for ingestion\n"
    );
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_missing_json_streams_like_loki() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(json!({}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::UNPROCESSABLE_ENTITY);
    check!(
        text_body(response).await == "error at least one valid stream is required for ingestion\n"
    );
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_empty_json_streams_like_loki() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": []
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::UNPROCESSABLE_ENTITY);
    check!(
        text_body(response).await == "error at least one valid stream is required for ingestion\n"
    );
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_accepts_missing_json_values_like_loki() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                }
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::NO_CONTENT);
    check!(text_body(response).await.is_empty());
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_accepts_null_json_values_like_loki() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": null
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::NO_CONTENT);
    check!(text_body(response).await.is_empty());
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_non_array_json_values_like_loki() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": "not-values"
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = text_body(response).await;
    check!(body.contains(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Unknown value type"
    ));
    check!(body.contains("not-values"));
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_non_object_json_labels_like_loki() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": "not-labels",
                                "values": [["19", "labels field is not an object"]]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = text_body(response).await;
    check!(body.contains(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like object"
    ));
    check!(body.contains("labels field is not an object"));
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_null_json_labels_like_loki() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": null,
                                "values": [["19", "null labels field"]]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = text_body(response).await;
    check!(body.contains(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like object"
    ));
    check!(body.contains("null labels field"));
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_accepts_missing_json_labels_like_loki() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "values": [["19", "missing labels field"]]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::NO_CONTENT);
    check!(text_body(response).await.is_empty());
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_returns_server_error_when_wal_append_fails() {
    let app = distributor_router(FailingWalSink);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api",
                                    "env": "prod"
                                },
                                "values": [["19", "api error"]]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::SERVICE_UNAVAILABLE);
    assert_loki_error(&json_body(response).await, "server_error", "wal sink");
}

/// A push whose entries appended in part must not reach the client as a
/// success. Loki has no way to tell Promtail or Alloy that half a push landed,
/// so both retry the whole push on a 5xx, and both read a 2xx as "every entry
/// is durable". The status stays 503 and the error names how far the batch
/// got, because the logs read path has no query-time deduplication: the retry
/// writes the entries that already landed a second time, permanently.
#[tokio::test]
async fn loki_push_endpoint_reports_a_partial_wal_batch_as_a_failure() {
    let app = distributor_router(PartialWalSink::new(2));

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": { "app": "api", "env": "prod" },
                                "values": [
                                    ["19", "one"],
                                    ["20", "two"],
                                    ["21", "three"],
                                    ["22", "four"],
                                    ["23", "five"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::SERVICE_UNAVAILABLE);
    assert_loki_error(
        &json_body(response).await,
        "server_error",
        "wal append wrote 2 of 5 records",
    );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_gzipped_json_payloads() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api",
                    "env": "prod"
                },
                "values": [
                    ["19", "api error", {"trace_id": "abc"}]
                ]
            }
        ]
    })
    .to_string();
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(payload.as_bytes()).unwrap();
    let payload = encoder.finish().unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .header("content-encoding", "gzip")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(
        records
            == vec![WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([
                    ("app", "api"),
                    ("detected_level", "error"),
                    ("env", "prod"),
                    ("service_name", "api"),
                ]),
                timestamp_ns: 19,
                line: "api error".to_string(),
                structured_metadata: BTreeMap::from([("trace_id".to_string(), "abc".to_string())]),
                position: None,
            }]
    );
}

#[tokio::test]
async fn loki_push_endpoint_rejects_malformed_gzip_payload_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .header("content-encoding", "gzip")
                .body(Body::from("not gzip"))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::BAD_REQUEST);
    check!(text_body(response).await == "unexpected EOF\n");
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_accepts_deflated_json_payloads() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api",
                    "env": "prod"
                },
                "values": [
                    ["19", "api error", {"trace_id": "abc"}]
                ]
            }
        ]
    })
    .to_string();
    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(payload.as_bytes()).unwrap();
    let payload = encoder.finish().unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .header("content-encoding", "deflate")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(
        records
            == vec![WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([
                    ("app", "api"),
                    ("detected_level", "error"),
                    ("env", "prod"),
                    ("service_name", "api"),
                ]),
                timestamp_ns: 19,
                line: "api error".to_string(),
                structured_metadata: BTreeMap::from([("trace_id".to_string(), "abc".to_string())]),
                position: None,
            }]
    );
}

#[tokio::test]
async fn loki_push_endpoint_rejects_malformed_deflate_payload_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .header("content-encoding", "deflate")
                .body(Body::from("not deflate"))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::BAD_REQUEST);
    check!(text_body(response).await == "EOF\n");
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_unsupported_content_encoding_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .header("content-encoding", "br")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["19", "api error"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::BAD_REQUEST);
    check!(text_body(response).await == "Content-Encoding \"br\" not supported\n");
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_treats_non_json_content_type_as_snappy_protobuf() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "text/plain")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["19", "api error"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::BAD_REQUEST);
    // Real Loki 3.4.2 returns a plain-text body (`Content-Type: text/plain`) for a
    // failed snappy-protobuf decode on a non-JSON push, not a JSON error envelope.
    check!(text_body(response).await == "snappy: corrupt input\n");
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_malformed_content_type_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json; charset")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["19", "api error"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "invalid media");
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_accepts_json_content_type_parameters() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json; charset=utf-8")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["19", "api error"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(
        records
            == vec![WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([
                    ("app", "api"),
                    ("detected_level", "error"),
                    ("service_name", "api"),
                ]),
                timestamp_ns: 19,
                line: "api error".to_string(),
                structured_metadata: BTreeMap::new(),
                position: None,
            }]
    );
}

#[tokio::test]
async fn deprecated_api_prom_push_endpoint_writes_wal_records() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/prom/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api",
                                    "env": "prod"
                                },
                                "values": [
                                    ["19", "api error"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(
        records
            == vec![WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([
                    ("app", "api"),
                    ("detected_level", "error"),
                    ("env", "prod"),
                    ("service_name", "api"),
                ]),
                timestamp_ns: 19,
                line: "api error".to_string(),
                structured_metadata: BTreeMap::new(),
                position: None,
            }]
    );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_snappy_protobuf_payloads() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = LokiProtoPushRequest {
        streams: vec![LokiProtoStream {
            labels: r#"{app="api", env="prod"}"#.to_string(),
            entries: vec![LokiProtoEntry {
                timestamp: Some(LokiProtoTimestamp {
                    seconds: 0,
                    nanos: 19,
                }),
                line: "api error".to_string(),
                structured_metadata: vec![
                    LokiProtoLabelPair {
                        name: "status".to_string(),
                        value: "500".to_string(),
                    },
                    LokiProtoLabelPair {
                        name: "trace_id".to_string(),
                        value: "abc".to_string(),
                    },
                ],
                parsed: vec![],
            }],
            hash: 0,
        }],
    };
    let payload = SnappyEncoder::new()
        .compress_vec(&payload.encode_to_vec())
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(
        records
            == vec![WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([
                    ("app", "api"),
                    ("detected_level", "error"),
                    ("env", "prod"),
                    ("service_name", "api"),
                ]),
                timestamp_ns: 19,
                line: "api error".to_string(),
                structured_metadata: BTreeMap::from([
                    ("status".to_string(), "500".to_string()),
                    ("trace_id".to_string(), "abc".to_string()),
                ]),
                position: None,
            }]
    );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_empty_protobuf_labels_with_unknown_service() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = LokiProtoPushRequest {
        streams: vec![LokiProtoStream {
            labels: "{}".to_string(),
            entries: vec![LokiProtoEntry {
                timestamp: Some(LokiProtoTimestamp {
                    seconds: 0,
                    nanos: 19,
                }),
                line: "api error".to_string(),
                structured_metadata: vec![],
                parsed: vec![],
            }],
            hash: 0,
        }],
    };
    let payload = SnappyEncoder::new()
        .compress_vec(&payload.encode_to_vec())
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(
        records
            == vec![WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([
                    ("detected_level", "error"),
                    ("service_name", "unknown_service"),
                ]),
                timestamp_ns: 19,
                line: "api error".to_string(),
                structured_metadata: BTreeMap::new(),
                position: None,
            }]
    );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_empty_string_protobuf_labels_with_unknown_service() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = LokiProtoPushRequest {
        streams: vec![LokiProtoStream {
            labels: String::new(),
            entries: vec![LokiProtoEntry {
                timestamp: Some(LokiProtoTimestamp {
                    seconds: 0,
                    nanos: 19,
                }),
                line: "api error".to_string(),
                structured_metadata: vec![],
                parsed: vec![],
            }],
            hash: 0,
        }],
    };
    let payload = SnappyEncoder::new()
        .compress_vec(&payload.encode_to_vec())
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(
        records
            == vec![WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([
                    ("detected_level", "error"),
                    ("service_name", "unknown_service"),
                ]),
                timestamp_ns: 19,
                line: "api error".to_string(),
                structured_metadata: BTreeMap::new(),
                position: None,
            }]
    );
}

#[tokio::test]
async fn loki_push_endpoint_rejects_invalid_snappy_protobuf_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(vec![0xff, 0xff, 0xff]))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::BAD_REQUEST);
    check!(text_body(response).await == "snappy: corrupt input\n");
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_invalid_protobuf_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(vec![0x03, 0x08, 0xff, 0xff, 0xff]))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::BAD_REQUEST);
    check!(text_body(response).await == "unexpected EOF\n");
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_empty_protobuf_push_like_loki_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = LokiProtoPushRequest { streams: vec![] };
    let payload = SnappyEncoder::new()
        .compress_vec(&payload.encode_to_vec())
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::UNPROCESSABLE_ENTITY);
    check!(
        text_body(response).await == "error at least one valid stream is required for ingestion\n"
    );
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_invalid_protobuf_labels_like_loki_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = LokiProtoPushRequest {
        streams: vec![LokiProtoStream {
            labels: r#"{bad-label="api"}"#.to_string(),
            entries: vec![LokiProtoEntry {
                timestamp: Some(LokiProtoTimestamp {
                    seconds: 0,
                    nanos: 19,
                }),
                line: "api error".to_string(),
                structured_metadata: vec![],
                parsed: vec![],
            }],
            hash: 0,
        }],
    };
    let payload = SnappyEncoder::new()
        .compress_vec(&payload.encode_to_vec())
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::BAD_REQUEST);
    check!(
        text_body(response).await
            == "couldn't parse labels: 1:5: parse error: unexpected character inside braces: '-'\n"
    );
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_duplicate_protobuf_labels_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = LokiProtoPushRequest {
        streams: vec![LokiProtoStream {
            labels: r#"{app="api", app="worker"}"#.to_string(),
            entries: vec![LokiProtoEntry {
                timestamp: Some(LokiProtoTimestamp {
                    seconds: 0,
                    nanos: 19,
                }),
                line: "api error".to_string(),
                structured_metadata: vec![],
                parsed: vec![],
            }],
            hash: 0,
        }],
    };
    let payload = SnappyEncoder::new()
        .compress_vec(&payload.encode_to_vec())
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::BAD_REQUEST);
    check!(
        text_body(response).await
            == "stream '{app=\"api\", app=\"worker\", service_name=\"api\"}' has duplicate label name: 'app'\n"
    );
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_accepts_duplicate_protobuf_structured_metadata_using_last_value() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = LokiProtoPushRequest {
        streams: vec![LokiProtoStream {
            labels: r#"{app="api"}"#.to_string(),
            entries: vec![LokiProtoEntry {
                timestamp: Some(LokiProtoTimestamp {
                    seconds: 0,
                    nanos: 19,
                }),
                line: "api error".to_string(),
                structured_metadata: vec![
                    LokiProtoLabelPair {
                        name: "trace_id".to_string(),
                        value: "abc".to_string(),
                    },
                    LokiProtoLabelPair {
                        name: "trace_id".to_string(),
                        value: "def".to_string(),
                    },
                ],
                parsed: vec![],
            }],
            hash: 0,
        }],
    };
    let payload = SnappyEncoder::new()
        .compress_vec(&payload.encode_to_vec())
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(
        records
            == vec![WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([
                    ("app", "api"),
                    ("detected_level", "error"),
                    ("service_name", "api"),
                ]),
                timestamp_ns: 19,
                line: "api error".to_string(),
                structured_metadata: BTreeMap::from([("trace_id".to_string(), "def".to_string())]),
                position: None,
            }]
    );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_invalid_protobuf_structured_metadata_name() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = LokiProtoPushRequest {
        streams: vec![LokiProtoStream {
            labels: r#"{app="api"}"#.to_string(),
            entries: vec![LokiProtoEntry {
                timestamp: Some(LokiProtoTimestamp {
                    seconds: 0,
                    nanos: 19,
                }),
                line: "api error".to_string(),
                structured_metadata: vec![LokiProtoLabelPair {
                    name: "9bad".to_string(),
                    value: "metadata".to_string(),
                }],
                parsed: vec![],
            }],
            hash: 0,
        }],
    };
    let payload = SnappyEncoder::new()
        .compress_vec(&payload.encode_to_vec())
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(
        records
            == vec![WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([
                    ("app", "api"),
                    ("detected_level", "error"),
                    ("service_name", "api"),
                ]),
                timestamp_ns: 19,
                line: "api error".to_string(),
                structured_metadata: BTreeMap::from([("9bad".to_string(), "metadata".to_string())]),
                position: None,
            }]
    );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_empty_protobuf_structured_metadata_name() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = LokiProtoPushRequest {
        streams: vec![LokiProtoStream {
            labels: r#"{app="api"}"#.to_string(),
            entries: vec![LokiProtoEntry {
                timestamp: Some(LokiProtoTimestamp {
                    seconds: 0,
                    nanos: 19,
                }),
                line: "api error".to_string(),
                structured_metadata: vec![LokiProtoLabelPair {
                    name: String::new(),
                    value: "metadata".to_string(),
                }],
                parsed: vec![],
            }],
            hash: 0,
        }],
    };
    let payload = SnappyEncoder::new()
        .compress_vec(&payload.encode_to_vec())
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(
        records
            == vec![WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([
                    ("app", "api"),
                    ("detected_level", "error"),
                    ("service_name", "api"),
                ]),
                timestamp_ns: 19,
                line: "api error".to_string(),
                structured_metadata: BTreeMap::from([(String::new(), "metadata".to_string())]),
                position: None,
            }]
    );
}

#[tokio::test]
async fn loki_push_endpoint_rejects_invalid_timestamp_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["not-a-timestamp", "api error"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_invalid_json_timestamp_like_loki_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["not-a-timestamp", "invalid push timestamp"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::BAD_REQUEST);
    check!(
        text_body(response).await
            == "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}' symbol, error found in #10 byte of ...|estamp\"]]}]}|..., bigger context ...|s\":[[\"not-a-timestamp\",\"invalid push timestamp\"]]}]}|...\n"
    );
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_non_string_json_timestamp_like_loki_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    [1_000_000_000, "non-string push timestamp"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::BAD_REQUEST);
    check!(
        text_body(response).await
            == "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}' symbol, error found in #10 byte of ...|estamp\"]]}]}|..., bigger context ...|alues\":[[1000000000,\"non-string push timestamp\"]]}]}|...\n"
    );
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_object_json_timestamp_like_loki_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    [{"ts": "1000000000"}, "object push timestamp"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::BAD_REQUEST);
    check!(
        text_body(response).await
            == "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}' symbol, error found in #10 byte of ...|estamp\"]]}]}|..., bigger context ...|\":[[{\"ts\":\"1000000000\"},\"object push timestamp\"]]}]}|...\n"
    );
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_array_json_timestamp_like_loki_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    [["1000000000"], "array push timestamp"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::BAD_REQUEST);
    check!(
        text_body(response).await
            == "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}' symbol, error found in #10 byte of ...|estamp\"]]}]}|..., bigger context ...|values\":[[[\"1000000000\"],\"array push timestamp\"]]}]}|...\n"
    );
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_invalid_json_line_like_loki_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["1000000000", 500]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::BAD_REQUEST);
    check!(
        text_body(response).await
            == "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value is string, but can't find closing '\"' symbol, error found in #10 byte of ...|00\",500]]}]}|..., bigger context ...|ream\":{\"app\":\"api\"},\"values\":[[\"1000000000\",500]]}]}|...\n"
    );
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_negative_timestamp_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["-1", "api error"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "timestamp");
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_negative_protobuf_timestamp_like_loki_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let config = ServiceConfig {
        target: Role::Distributor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_length: None,
        max_ingest_body: None,
        wal_append_timeout: None,
        ..ServiceConfig::default()
    };
    let app = build_service_router(
        &config,
        ServiceDependencies::default().with_wal_sink(sink.clone()),
        None,
    )
    .await
    .unwrap();
    let payload = LokiProtoPushRequest {
        streams: vec![LokiProtoStream {
            labels: r#"{app="api"}"#.to_string(),
            entries: vec![LokiProtoEntry {
                timestamp: Some(LokiProtoTimestamp {
                    seconds: -1,
                    nanos: 0,
                }),
                line: "api error".to_string(),
                structured_metadata: vec![],
                parsed: vec![],
            }],
            hash: 0,
        }],
    };
    let payload = SnappyEncoder::new()
        .compress_vec(&payload.encode_to_vec())
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = text_body(response).await;
    check!(body.contains("timestamp too old"));
    check!(body.contains("1969-12-31T23:59:59Z"));
    check!(body.contains(r#"{app="api", service_name="api"}"#));
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_out_of_range_protobuf_timestamp_nanos_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = LokiProtoPushRequest {
        streams: vec![LokiProtoStream {
            labels: r#"{app="api"}"#.to_string(),
            entries: vec![LokiProtoEntry {
                timestamp: Some(LokiProtoTimestamp {
                    seconds: 0,
                    nanos: 1_000_000_000,
                }),
                line: "api error".to_string(),
                structured_metadata: vec![],
                parsed: vec![],
            }],
            hash: 0,
        }],
    };
    let payload = SnappyEncoder::new()
        .compress_vec(&payload.encode_to_vec())
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "timestamp");
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_invalid_json_labels_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "bad-label": "api"
                                },
                                "values": [
                                    ["19", "api error"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::BAD_REQUEST);
    check!(
        text_body(response).await
            == "couldn't parse labels: 1:5: parse error: unexpected character inside braces: '-'\n"
    );
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_accepts_duplicate_json_labels_using_last_value() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{
                        "streams": [
                            {
                                "stream": {
                                    "app": "api",
                                    "app": "worker"
                                },
                                "values": [
                                    ["19", "api error"]
                                ]
                            }
                        ]
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(
        records[0].labels
            == labels([
                ("app", "worker"),
                ("detected_level", "error"),
                ("service_name", "worker"),
            ])
    );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_empty_json_labels_with_unknown_service() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {},
                                "values": [
                                    ["19", "api info"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(
        records[0].labels
            == labels([
                ("detected_level", "info"),
                ("service_name", "unknown_service"),
            ])
    );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_invalid_json_structured_metadata_name() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["19", "api error", {"9bad": "metadata"}]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(
        records
            == vec![WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([
                    ("app", "api"),
                    ("detected_level", "error"),
                    ("service_name", "api"),
                ]),
                timestamp_ns: 19,
                line: "api error".to_string(),
                structured_metadata: BTreeMap::from([("9bad".to_string(), "metadata".to_string())]),
                position: None,
            }]
    );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_duplicate_json_structured_metadata_using_last_value() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    [
                                        "19",
                                        "api error",
                                        {
                                            "trace_id": "abc",
                                            "trace_id": "def"
                                        }
                                    ]
                                ]
                            }
                        ]
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(
        records
            == vec![WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([
                    ("app", "api"),
                    ("detected_level", "error"),
                    ("service_name", "api"),
                ]),
                timestamp_ns: 19,
                line: "api error".to_string(),
                structured_metadata: BTreeMap::from([("trace_id".to_string(), "def".to_string())]),
                position: None,
            }]
    );
}

#[tokio::test]
async fn loki_push_endpoint_rejects_non_string_json_structured_metadata_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    for structured_metadata in [json!({"status": 500}), json!({"nested": {"status": "500"}})] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/loki/api/v1/push")
                    .header("X-Scope-OrgID", "tenant-a")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "streams": [
                                {
                                    "stream": {
                                        "app": "api"
                                    },
                                    "values": [
                                        ["19", "api error", structured_metadata]
                                    ]
                                }
                            ]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(text_body(response).await.contains(
            "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value is string"
        ));
    }

    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_non_object_json_structured_metadata_like_loki() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["19", "api error", null]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = text_body(response).await;
    check!(body.contains(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like object"
    ));
    check!(body.contains("api error\",null"));
    check!(sink.records().is_empty());
}
