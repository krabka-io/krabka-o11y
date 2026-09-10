//! The OTLP log endpoints over HTTP and gRPC.

mod support;

use std::collections::BTreeMap;

use assert2::{assert, check};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use krabka_blockstore::labels;
use krabka_observability::{
    InMemoryWalSink, WalLogRecord, distributor_router, otlp_grpc_logs_service,
    otlp_grpc_logs_service_with_limiter,
};
use opentelemetry_proto::tonic::{
    collector::logs::v1::logs_service_server::LogsService, common::v1::any_value,
    resource::v1::Resource,
};
use prost::Message as _;
use serde_json::json;
use support::{
    FailingWalSink, RejectingIngestLimiter, assert_loki_error, json_body, proto_key_value,
    proto_logs_request,
};
use tower::ServiceExt as _;

#[tokio::test]
async fn otlp_logs_endpoint_writes_tenant_scoped_wal_records() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "resource": {
                                    "attributes": [
                                        {"key": "service.name", "value": {"stringValue": "checkout"}},
                                        {"key": "deployment.environment", "value": {"stringValue": "prod"}}
                                    ]
                                },
                                "scopeLogs": [
                                    {
                                        "scope": {
                                            "attributes": [
                                                {"key": "instrumentation.scope", "value": {"stringValue": "api"}}
                                            ]
                                        },
                                        "logRecords": [
                                            {
                                                "timeUnixNano": "19",
                                                "body": {"stringValue": "api error"},
                                                "attributes": [
                                                    {"key": "status", "value": {"intValue": "500"}},
                                                    {"key": "trace_id", "value": {"stringValue": "abc"}}
                                                ]
                                            }
                                        ]
                                    }
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
                    ("deployment_environment", "prod"),
                    ("instrumentation_scope", "api"),
                    ("service_name", "checkout"),
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
async fn otlp_logs_endpoint_preserves_severity_fields_as_structured_metadata() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "resource": {
                                    "attributes": [
                                        {"key": "service.name", "value": {"stringValue": "checkout"}}
                                    ]
                                },
                                "scopeLogs": [
                                    {
                                        "logRecords": [
                                            {
                                                "timeUnixNano": "19",
                                                "severityText": "ERROR",
                                                "severityNumber": 17,
                                                "body": {"stringValue": "api error"},
                                                "attributes": [
                                                    {"key": "trace_id", "value": {"stringValue": "abc"}}
                                                ]
                                            }
                                        ]
                                    }
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
        records[0].structured_metadata
            == BTreeMap::from([
                ("severity_number".to_string(), "17".to_string()),
                ("severity_text".to_string(), "ERROR".to_string()),
                ("trace_id".to_string(), "abc".to_string()),
            ])
    );
}

#[tokio::test]
async fn otlp_logs_endpoint_returns_server_error_when_wal_append_fails() {
    let app = distributor_router(FailingWalSink);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "resource": {
                                    "attributes": [
                                        {"key": "service.name", "value": {"stringValue": "checkout"}}
                                    ]
                                },
                                "scopeLogs": [
                                    {
                                        "logRecords": [
                                            {
                                                "timeUnixNano": "19",
                                                "body": {"stringValue": "api error"}
                                            }
                                        ]
                                    }
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
    assert_loki_error(&json_body(response).await, "server_error", "wal sink");
}

#[tokio::test]
async fn otlp_logs_endpoint_normalizes_attribute_names_for_loki_labels_and_metadata() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "resource": {
                                    "attributes": [
                                        {"key": "service.name", "value": {"stringValue": "checkout"}},
                                        {"key": "cloud/region", "value": {"stringValue": "us-west"}}
                                    ]
                                },
                                "scopeLogs": [
                                    {
                                        "scope": {
                                            "attributes": [
                                                {"key": "instrumentation.scope", "value": {"stringValue": "api"}}
                                            ]
                                        },
                                        "logRecords": [
                                            {
                                                "timeUnixNano": "19",
                                                "body": {"stringValue": "api error"},
                                                "attributes": [
                                                    {"key": "thread.name", "value": {"stringValue": "worker-1"}},
                                                    {"key": "http.status-code", "value": {"intValue": "500"}}
                                                ]
                                            }
                                        ]
                                    }
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
                    ("cloud_region", "us-west"),
                    ("instrumentation_scope", "api"),
                    ("service_name", "checkout"),
                ]),
                timestamp_ns: 19,
                line: "api error".to_string(),
                structured_metadata: BTreeMap::from([
                    ("http_status_code".to_string(), "500".to_string()),
                    ("thread_name".to_string(), "worker-1".to_string()),
                ]),
                position: None,
            }]
    );
}

#[tokio::test]
async fn otlp_logs_endpoint_rejects_duplicate_normalized_resource_attributes_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "resource": {
                                    "attributes": [
                                        {"key": "service.name", "value": {"stringValue": "checkout"}},
                                        {"key": "service_name", "value": {"stringValue": "billing"}}
                                    ]
                                },
                                "scopeLogs": [
                                    {
                                        "logRecords": [
                                            {
                                                "timeUnixNano": "19",
                                                "body": {"stringValue": "api error"}
                                            }
                                        ]
                                    }
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
    assert_loki_error(&json_body(response).await, "bad_data", "OTLP attribute");
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn otlp_logs_endpoint_rejects_duplicate_normalized_log_attributes_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "resource": {
                                    "attributes": [
                                        {"key": "service.name", "value": {"stringValue": "checkout"}}
                                    ]
                                },
                                "scopeLogs": [
                                    {
                                        "logRecords": [
                                            {
                                                "timeUnixNano": "19",
                                                "body": {"stringValue": "api error"},
                                                "attributes": [
                                                    {"key": "trace.id", "value": {"stringValue": "abc"}},
                                                    {"key": "trace_id", "value": {"stringValue": "def"}}
                                                ]
                                            }
                                        ]
                                    }
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
    assert_loki_error(&json_body(response).await, "bad_data", "OTLP attribute");
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn otlp_logs_endpoint_discovers_service_name_label_from_resource_attributes() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "resource": {
                                    "attributes": [
                                        {"key": "app", "value": {"stringValue": "checkout"}},
                                        {"key": "deployment.environment", "value": {"stringValue": "prod"}}
                                    ]
                                },
                                "scopeLogs": [
                                    {
                                        "logRecords": [
                                            {
                                                "timeUnixNano": "19",
                                                "body": {"stringValue": "api error"}
                                            }
                                        ]
                                    }
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
                ("app", "checkout"),
                ("deployment_environment", "prod"),
                ("service_name", "checkout"),
            ])
    );
}

#[tokio::test]
async fn otlp_logs_endpoint_uses_unknown_service_when_no_service_name_candidate_exists() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "scopeLogs": [
                                    {
                                        "logRecords": [
                                            {
                                                "timeUnixNano": "19",
                                                "body": {"stringValue": "api error"}
                                            }
                                        ]
                                    }
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
    assert!(records[0].labels == labels([("service_name", "unknown_service")]));
}

#[tokio::test]
async fn otlp_logs_endpoint_rejects_invalid_timestamp_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "resource": {
                                    "attributes": [
                                        {"key": "service.name", "value": {"stringValue": "checkout"}}
                                    ]
                                },
                                "scopeLogs": [
                                    {
                                        "logRecords": [
                                            {
                                                "timeUnixNano": "not-a-timestamp",
                                                "body": {"stringValue": "api error"}
                                            }
                                        ]
                                    }
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
async fn otlp_logs_endpoint_rejects_negative_timestamp_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "resource": {
                                    "attributes": [
                                        {"key": "service.name", "value": {"stringValue": "checkout"}}
                                    ]
                                },
                                "scopeLogs": [
                                    {
                                        "logRecords": [
                                            {
                                                "timeUnixNano": "-1",
                                                "body": {"stringValue": "api error"}
                                            }
                                        ]
                                    }
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
async fn otlp_logs_endpoint_accepts_protobuf_payloads() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = proto_logs_request().encode_to_vec();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
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
                    ("deployment_environment", "prod"),
                    ("instrumentation_scope", "api"),
                    ("service_name", "checkout"),
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
async fn otlp_logs_endpoint_maps_proto_trace_and_span_ids_to_structured_metadata() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let mut request = proto_logs_request();
    let log = &mut request.resource_logs[0].scope_logs[0].log_records[0];
    log.attributes = vec![proto_key_value("status", any_value::Value::IntValue(500))];
    log.trace_id = vec![
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10,
    ];
    log.span_id = vec![0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18];

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(request.encode_to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(
        records[0].structured_metadata
            == BTreeMap::from([
                ("status".to_string(), "500".to_string()),
                (
                    "trace_id".to_string(),
                    "0102030405060708090a0b0c0d0e0f10".to_string(),
                ),
                ("span_id".to_string(), "1112131415161718".to_string()),
            ])
    );
}

#[tokio::test]
async fn otlp_logs_endpoint_maps_proto_severity_fields_to_structured_metadata() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let mut request = proto_logs_request();
    let log = &mut request.resource_logs[0].scope_logs[0].log_records[0];
    log.severity_number = 17;
    log.severity_text = "ERROR".to_string();
    log.attributes = vec![proto_key_value(
        "trace_id",
        any_value::Value::StringValue("abc".into()),
    )];

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(request.encode_to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(
        records[0].structured_metadata
            == BTreeMap::from([
                ("severity_number".to_string(), "17".to_string()),
                ("severity_text".to_string(), "ERROR".to_string()),
                ("trace_id".to_string(), "abc".to_string()),
            ])
    );
}

#[tokio::test]
async fn otlp_logs_endpoint_rejects_duplicate_normalized_protobuf_attributes_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let mut request = proto_logs_request();
    request.resource_logs[0].resource = Some(Resource {
        attributes: vec![
            proto_key_value(
                "service.name",
                any_value::Value::StringValue("checkout".into()),
            ),
            proto_key_value(
                "service_name",
                any_value::Value::StringValue("billing".into()),
            ),
        ],
        dropped_attributes_count: 0,
        entity_refs: vec![],
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(request.encode_to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "OTLP attribute");
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn otlp_logs_endpoint_accepts_loki_otlp_path() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = proto_logs_request().encode_to_vec();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/otlp/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    check!(records.len() == 1);
    check!(records[0].tenant == "tenant-a");
    check!(records[0].line == "api error");
}

#[tokio::test]
async fn otlp_grpc_logs_service_writes_tenant_scoped_wal_records() {
    let sink = InMemoryWalSink::default();
    let service = otlp_grpc_logs_service(sink.clone());
    let mut request = tonic::Request::new(proto_logs_request());
    request
        .metadata_mut()
        .insert("x-scope-orgid", "tenant-a".parse().unwrap());

    let response = service.export(request).await.unwrap();

    assert!(response.get_ref().partial_success.is_none());
    let records = sink.records();
    assert!(
        records
            == vec![WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([
                    ("deployment_environment", "prod"),
                    ("instrumentation_scope", "api"),
                    ("service_name", "checkout"),
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
async fn otlp_grpc_logs_service_rejects_missing_tenant_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let service = otlp_grpc_logs_service(sink.clone());

    let error = service
        .export(tonic::Request::new(proto_logs_request()))
        .await
        .unwrap_err();

    assert!(error.code() == tonic::Code::InvalidArgument);
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn otlp_grpc_logs_service_rejects_ingest_quota_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let service = otlp_grpc_logs_service_with_limiter(sink.clone(), RejectingIngestLimiter);
    let mut request = tonic::Request::new(proto_logs_request());
    request
        .metadata_mut()
        .insert("x-scope-orgid", "tenant-a".parse().unwrap());

    let error = service.export(request).await.unwrap_err();

    check!(error.code() == tonic::Code::ResourceExhausted);
    check!(error.message().contains("tenant write quota exceeded"));
    check!(sink.records().is_empty());
}

#[tokio::test]
async fn otlp_logs_endpoint_rejects_invalid_protobuf_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(vec![0xff, 0xff, 0xff]))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(sink.records().is_empty());
}
