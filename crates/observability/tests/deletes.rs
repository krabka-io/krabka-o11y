//! The compactor delete API, and the querier results a delete request filters.

mod support;

use std::collections::BTreeMap;

use assert2::{assert, check};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use futures_util::StreamExt as _;
use krabka_blockstore::{
    BlockKey, LabelIndex, LogBlockIndex as BlockIndex, LogRow, TimeRange, labels, write_log_block,
    write_log_index_manifest,
};
use krabka_observability::{
    InMemoryWalSink, LogWalSink, QuerierIndexSource, Role, ServiceConfig, ServiceDependencies,
    SharedLogDeleteRequests, WalLogRecord, build_service_router,
};
use krabka_units::convert::ByteSizeExt as _;
use serde_json::{Value, json};
use support::{expected_loki_stats_with, json_body, test_service_config, text_body};
use tokio::{
    net::TcpListener,
    time::{Duration, timeout},
};
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest as _};
use tower::ServiceExt as _;

#[tokio::test]
async fn compactor_delete_endpoint_tracks_and_cancels_delete_requests() {
    let dir = tempfile::tempdir().unwrap().keep();
    let config = ServiceConfig {
        target: Role::Compactor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-compactor".to_string(),
        data_root: dir,
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: Some("observability/logs".to_string()),
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
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22secret%22&start=1591616227&end=1591619692")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(create_response.status() == StatusCode::NO_CONTENT);

    let list_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/delete")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(list_response.status() == StatusCode::OK);
    let body = json_body(list_response).await;
    check!(body.as_array().unwrap().len() == 1);
    check!(body[0]["request_id"] == "delete-1");
    check!(body[0]["query"] == "{app=\"api\"} |= \"secret\"");
    check!(body[0]["start_time"] == 1_591_616_227_i64);
    check!(body[0]["end_time"] == 1_591_619_692_i64);
    check!(body[0]["status"] == "received");
    check!(body[0]["created_at"].as_i64().is_some());

    let other_tenant_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/delete")
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(json_body(other_tenant_response).await == json!([]));

    let cancel_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/loki/api/v1/delete?request_id=delete-1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(cancel_response.status() == StatusCode::NO_CONTENT);

    let list_after_cancel_response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/delete")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(json_body(list_after_cancel_response).await == json!([]));
}

#[tokio::test]
async fn compactor_delete_endpoint_accepts_form_post_query_with_raw_ampersand() {
    let dir = tempfile::tempdir().unwrap().keep();
    let config = ServiceConfig {
        target: Role::Compactor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-compactor".to_string(),
        data_root: dir,
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: Some("observability/logs".to_string()),
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
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api&edge"} |= "secret"&start=1591616227&end=1591619692"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(create_response.status() == StatusCode::NO_CONTENT);

    let list_response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/delete")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(list_response.status() == StatusCode::OK);
    let body = json_body(list_response).await;
    check!(body.as_array().unwrap().len() == 1);
    check!(body[0]["query"] == r#"{app="api&edge"} |= "secret""#);
    check!(body[0]["start_time"] == 1_591_616_227_i64);
    check!(body[0]["end_time"] == 1_591_619_692_i64);
}

#[tokio::test]
async fn compactor_delete_endpoint_rejects_invalid_requests() {
    let config = ServiceConfig {
        target: Role::Compactor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: Some("observability/logs".to_string()),
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
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let missing_tenant_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D&start=1591616227")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(missing_tenant_response.status() == StatusCode::BAD_REQUEST);
    assert!(
        json_body(missing_tenant_response).await["error"]
            .as_str()
            .unwrap()
            .contains("X-Scope-OrgID")
    );

    let missing_start_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(missing_start_response.status() == StatusCode::BAD_REQUEST);
    assert!(
        json_body(missing_start_response).await["error"]
            .as_str()
            .unwrap()
            .contains("start")
    );

    let invalid_query_response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=not-logql&start=1591616227")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(invalid_query_response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(invalid_query_response)
            .await
            .contains("parse error")
    );
}

#[tokio::test]
async fn compactor_delete_requests_filter_querier_stream_results() {
    let delete_requests = SharedLogDeleteRequests::default();
    let compactor_config = ServiceConfig {
        target: Role::Compactor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: Some("observability/logs".to_string()),
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
    let compactor_app = build_service_router(
        &compactor_config,
        ServiceDependencies::default().with_delete_requests(delete_requests.clone()),
        None,
    )
    .await
    .unwrap();

    let delete_response = compactor_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22secret%22&start=14&end=16")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete_response.status() == StatusCode::NO_CONTENT);

    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            14_000_000_000,
            17_000_000_000,
            TimeRange::new(14_000_000_000, 17_000_000_000).unwrap(),
        ),
        vec![
            LogRow::new(api, 14_000_000_000, "api ok", BTreeMap::new()),
            LogRow::new(api, 15_000_000_000, "api secret", BTreeMap::new()),
            LogRow::new(api, 17_000_000_000, "api later secret", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    let block_bytes = block.size.bytes_u64();
    block_index.insert(block);
    write_log_index_manifest(&dir, &label_index, &block_index).unwrap();
    let querier_config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-querier".to_string(),
        data_root: dir,
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
    let querier_app = build_service_router(
        &querier_config,
        ServiceDependencies::default().with_delete_requests(delete_requests),
        None,
    )
    .await
    .unwrap();

    let response = querier_app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=14000000000&end=17000000001&direction=forward")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body["data"]["result"]
            == json!([
                {
                    "stream": {
                        "app": "api",
                        "detected_level": "unknown",
                        "env": "prod"
                    },
                    "values": [
                        ["14000000000", "api ok"],
                        ["17000000000", "api later secret"]
                    ]
                }
            ])
    );
    assert!(body["data"]["stats"] == expected_loki_stats_with(block_bytes, 2, 1));
}

#[tokio::test]
async fn compactor_delete_requests_persist_for_configured_querier() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            14_000_000_000,
            17_000_000_000,
            TimeRange::new(14_000_000_000, 17_000_000_000).unwrap(),
        ),
        vec![
            LogRow::new(api, 14_000_000_000, "api ok", BTreeMap::new()),
            LogRow::new(api, 15_000_000_000, "api secret", BTreeMap::new()),
            LogRow::new(api, 17_000_000_000, "api later secret", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    let block_bytes = block.size.bytes_u64();
    block_index.insert(block);
    write_log_index_manifest(&dir, &label_index, &block_index).unwrap();

    let compactor_config = ServiceConfig {
        target: Role::Compactor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-compactor".to_string(),
        data_root: dir.clone(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: Some("observability/logs".to_string()),
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
    let compactor_app =
        build_service_router(&compactor_config, ServiceDependencies::default(), None)
            .await
            .unwrap();
    let delete_response = compactor_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22secret%22&start=14&end=16")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete_response.status() == StatusCode::NO_CONTENT);

    let querier_config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-querier".to_string(),
        data_root: dir,
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
    let querier_app = build_service_router(&querier_config, ServiceDependencies::default(), None)
        .await
        .unwrap();
    let response = querier_app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=14000000000&end=17000000001&direction=forward")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body["data"]["result"]
            == json!([
                {
                    "stream": {
                        "app": "api",
                        "detected_level": "unknown",
                        "env": "prod"
                    },
                    "values": [
                        ["14000000000", "api ok"],
                        ["17000000000", "api later secret"]
                    ]
                }
            ])
    );
    assert!(body["data"]["stats"] == expected_loki_stats_with(block_bytes, 2, 1));
}

#[tokio::test]
async fn compactor_delete_requests_filter_querier_metric_results() {
    let delete_requests = SharedLogDeleteRequests::default();
    let compactor_config = ServiceConfig {
        target: Role::Compactor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: Some("observability/logs".to_string()),
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
    let compactor_app = build_service_router(
        &compactor_config,
        ServiceDependencies::default().with_delete_requests(delete_requests.clone()),
        None,
    )
    .await
    .unwrap();

    let delete_response = compactor_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22secret%22&start=14&end=16")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete_response.status() == StatusCode::NO_CONTENT);

    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            14_000_000_000,
            17_000_000_000,
            TimeRange::new(14_000_000_000, 17_000_000_000).unwrap(),
        ),
        vec![
            LogRow::new(api, 14_000_000_000, "api ok", BTreeMap::new()),
            LogRow::new(api, 15_000_000_000, "api secret", BTreeMap::new()),
            LogRow::new(api, 17_000_000_000, "api later secret", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    let block_bytes = block.size.bytes_u64();
    block_index.insert(block);
    write_log_index_manifest(&dir, &label_index, &block_index).unwrap();
    let querier_config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-querier".to_string(),
        data_root: dir,
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
    let querier_app = build_service_router(
        &querier_config,
        ServiceDependencies::default().with_delete_requests(delete_requests),
        None,
    )
    .await
    .unwrap();

    let response = querier_app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B10s%5D%29&time=17000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "value": [17, "2"]
                        }
                    ],
                    "stats": expected_loki_stats_with(block_bytes, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn compactor_delete_requests_filter_querier_tail_results() {
    let delete_requests = SharedLogDeleteRequests::default();
    let compactor_config = ServiceConfig {
        target: Role::Compactor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: Some("observability/logs".to_string()),
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
    let compactor_app = build_service_router(
        &compactor_config,
        ServiceDependencies::default().with_delete_requests(delete_requests.clone()),
        None,
    )
    .await
    .unwrap();

    let delete_response = compactor_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22secret%22&start=14&end=16")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete_response.status() == StatusCode::NO_CONTENT);

    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    write_log_index_manifest(&dir, &label_index, &BlockIndex::default()).unwrap();

    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 15_000_000_000,
            line: "api secret".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 17_000_000_000,
            line: "api later secret".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();

    let querier_config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-querier-tail".to_string(),
        data_root: dir,
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
        &querier_config,
        ServiceDependencies::default()
            .with_hot_tail(hot_tail, 0)
            .with_delete_requests(delete_requests),
        None,
    )
    .await
    .unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut request =
        format!("ws://{addr}/loki/api/v1/tail?query=%7Bapp%3D%22api%22%7D&start=0&end=20000000000")
            .into_client_request()
            .unwrap();
    request
        .headers_mut()
        .insert("X-Scope-OrgID", "tenant-a".parse().unwrap());

    let (mut socket, response) = connect_async(request).await.unwrap();
    assert!(response.status() == StatusCode::SWITCHING_PROTOCOLS);
    let message = timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let frame: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    server.abort();

    assert!(
        frame
            == json!({
                "streams": [
                    {
                        "stream": {
                            "app": "api",
                            "detected_level": "unknown",
                            "env": "prod"
                        },
                        "values": [
                            ["17000000000", "api later secret"]
                        ]
                    }
                ]
            })
    );
}

#[tokio::test]
async fn compactor_delete_requests_filter_querier_patterns_results() {
    let delete_requests = SharedLogDeleteRequests::default();
    create_secret_delete_request(&delete_requests).await;

    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            14_000_000_000,
            17_000_000_000,
            TimeRange::new(14_000_000_000, 17_000_000_000).unwrap(),
        ),
        vec![
            LogRow::new(
                api,
                14_000_000_000,
                "status=500 user=100 secret",
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                17_000_000_000,
                "status=200 user=200 public",
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(block);
    write_log_index_manifest(&dir, &label_index, &block_index).unwrap();
    let querier_config = test_service_config(Role::Querier, dir);
    let querier_app = build_service_router(
        &querier_config,
        ServiceDependencies::default().with_delete_requests(delete_requests),
        None,
    )
    .await
    .unwrap();

    let response = querier_app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/patterns?query=%7Bapp%3D%22api%22%7D&start=14000000000&end=17000000001&step=1s")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": [
                    {
                        "pattern": "status=<_> user=<_> public",
                        "samples": [
                            [17, 1]
                        ]
                    }
                ]
            })
    );
}

#[tokio::test]
async fn compactor_delete_requests_filter_querier_detected_fields_results() {
    let delete_requests = SharedLogDeleteRequests::default();
    create_secret_delete_request(&delete_requests).await;

    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            14_000_000_000,
            17_000_000_000,
            TimeRange::new(14_000_000_000, 17_000_000_000).unwrap(),
        ),
        vec![
            LogRow::new(
                api,
                14_000_000_000,
                r#"{"status":"500","secret_field":"hidden","msg":"secret"}"#,
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                17_000_000_000,
                r#"{"status":"200","visible_field":"kept","msg":"public"}"#,
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(block);
    write_log_index_manifest(&dir, &label_index, &block_index).unwrap();
    let querier_config = test_service_config(Role::Querier, dir);
    let querier_app = build_service_router(
        &querier_config,
        ServiceDependencies::default().with_delete_requests(delete_requests),
        None,
    )
    .await
    .unwrap();

    let fields_response = querier_app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&start=14000000000&end=17000000000&limit=10")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(fields_response.status() == StatusCode::OK);
    assert!(
        json_body(fields_response).await
            == json!({
                "fields": [
                    {
                        "label": "detected_level",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": null
                    },
                    {
                        "label": "msg",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": ["json"]
                    },
                    {
                        "label": "status",
                        "type": "int",
                        "cardinality": 1,
                        "parsers": ["json"]
                    },
                    {
                        "label": "visible_field",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": ["json"]
                    }
                ],
                "limit": 10
            })
    );

    let values_response = querier_app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_field/status/values?query=%7Bapp%3D%22api%22%7D&start=14000000000&end=17000000000&limit=10")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(values_response.status() == StatusCode::OK);
    assert!(
        json_body(values_response).await
            == json!({
                "values": ["200"],
                "limit": 10
            })
    );
}

async fn create_secret_delete_request(delete_requests: &SharedLogDeleteRequests) {
    let compactor_config = test_service_config(Role::Compactor, ".");
    let compactor_app = build_service_router(
        &compactor_config,
        ServiceDependencies::default().with_delete_requests(delete_requests.clone()),
        None,
    )
    .await
    .unwrap();
    let delete_response = compactor_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22secret%22&start=14&end=16")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete_response.status() == StatusCode::NO_CONTENT);
}
