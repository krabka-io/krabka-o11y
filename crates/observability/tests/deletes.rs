//! The compactor delete API, and the querier results a delete request filters.

mod support;

use std::collections::BTreeMap;

use assert2::{assert, check};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use krabka_blockstore::{
    BlockKey, LabelIndex, LogBlockIndex as BlockIndex, LogRow, TimeRange, labels, write_log_block,
    write_log_index_manifest,
};
use krabka_observability::{
    InMemoryWalSink, LogWalSink, Role, ServiceConfig, ServiceDependencies, SharedLogDeleteRequests,
    WalLogRecord, build_service_router,
};
use krabka_units::convert::ByteSizeExt as _;
use serde_json::{Value, json};
use support::{
    LokiStatsCounts, LokiSuccess, Method, Tenant, TenantDenyingQueryAuthorizer, assert_loki_error,
    json_body, minimal_service_config, next_frame_within_two_seconds, open_tail, post_form,
    test_service_config, text_body,
};
use tower::ServiceExt as _;

#[tokio::test]
async fn compactor_delete_endpoint_tracks_and_cancels_delete_requests() {
    let app = compactor_app_over(tempfile::tempdir().unwrap().keep()).await;

    let create_response = Tenant("tenant-a").send(&app, Method::POST, "/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22secret%22&start=1591616227&end=1591619692").await;
    assert!(create_response.status() == StatusCode::NO_CONTENT);

    let body = only_tenant_a_delete_request(&app).await;
    check!(body["request_id"] == "delete-1");
    check!(body["query"] == "{app=\"api\"} |= \"secret\"");
    check!(body["start_time"] == 1_591_616_227_i64);
    check!(body["end_time"] == 1_591_619_692_i64);
    check!(body["status"] == "received");
    check!(body["created_at"].as_i64().is_some());

    let other_tenant_response = Tenant("tenant-b").get(&app, "/loki/api/v1/delete").await;
    assert!(json_body(other_tenant_response).await == json!([]));

    let cancel_response = Tenant("tenant-a")
        .send(
            &app,
            Method::DELETE,
            "/loki/api/v1/delete?request_id=delete-1",
        )
        .await;
    assert!(cancel_response.status() == StatusCode::NO_CONTENT);

    let list_after_cancel_response = Tenant("tenant-a").get(&app, "/loki/api/v1/delete").await;
    assert!(json_body(list_after_cancel_response).await == json!([]));
}

#[tokio::test]
async fn compactor_delete_endpoint_accepts_form_post_query_with_raw_ampersand() {
    let app = compactor_app_over(tempfile::tempdir().unwrap().keep()).await;

    let create_response = post_form(
        &app,
        "/loki/api/v1/delete",
        r#"query={app="api&edge"} |= "secret"&start=1591616227&end=1591619692"#,
    )
    .await;
    assert!(create_response.status() == StatusCode::NO_CONTENT);

    let body = only_tenant_a_delete_request(&app).await;
    check!(body["query"] == r#"{app="api&edge"} |= "secret""#);
    check!(body["start_time"] == 1_591_616_227_i64);
    check!(body["end_time"] == 1_591_619_692_i64);
}

#[tokio::test]
async fn compactor_delete_endpoint_rejects_invalid_requests() {
    let config = ServiceConfig {
        index_prefix: Some("observability/logs".to_string()),
        ..minimal_service_config(Role::BlockBuilder)
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
    // Loki's auth middleware answers before the handler reads a parameter.
    assert!(missing_tenant_response.status() == StatusCode::UNAUTHORIZED);
    assert!(text_body(missing_tenant_response).await == "no org id\n");

    let missing_start_response = Tenant("tenant-a")
        .send(
            &app,
            Method::POST,
            "/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D",
        )
        .await;
    assert!(missing_start_response.status() == StatusCode::BAD_REQUEST);
    assert!(
        json_body(missing_start_response).await["error"]
            .as_str()
            .unwrap()
            .contains("start")
    );

    let invalid_query_response = Tenant("tenant-a")
        .send(
            &app,
            Method::POST,
            "/loki/api/v1/delete?query=not-logql&start=1591616227",
        )
        .await;
    assert!(invalid_query_response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(invalid_query_response)
            .await
            .contains("parse error")
    );
}

#[tokio::test]
async fn compactor_delete_requests_filter_querier_stream_results() {
    let SecretLinesQuerier {
        querier_app,
        block_bytes,
    } = SecretLinesQuerier::after_a_secret_delete_request().await;

    let response = Tenant("tenant-a").get(&querier_app, "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=14000000000&end=17000000001&direction=forward").await;

    assert_secret_line_filtered(response, block_bytes).await;
}

#[tokio::test]
async fn compactor_delete_requests_persist_for_configured_querier() {
    let (dir, block_bytes) = secret_lines_manifest();

    let compactor_app = compactor_app_over(dir.clone()).await;
    let delete_response = Tenant("tenant-a").send(&compactor_app, Method::POST, "/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22secret%22&start=14&end=16").await;
    assert!(delete_response.status() == StatusCode::NO_CONTENT);

    let querier_app = querier_over(dir, ServiceDependencies::default()).await;
    let response = Tenant("tenant-a").get(&querier_app, "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=14000000000&end=17000000001&direction=forward").await;

    assert_secret_line_filtered(response, block_bytes).await;
}

#[tokio::test]
async fn compactor_delete_requests_filter_querier_metric_results() {
    let SecretLinesQuerier {
        querier_app,
        block_bytes,
    } = SecretLinesQuerier::after_a_secret_delete_request().await;

    let response = Tenant("tenant-a").get(&querier_app, "/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B10s%5D%29&time=17000000000").await;

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == LokiSuccess {
                result_type: "vector",
                data_result: json!([
                    {
                        "metric": {
                            "app": "api",
                            "env": "prod"
                        },
                        "value": [17, "2"]
                    }
                ]),
                stats: LokiStatsCounts {
                    store_bytes: block_bytes,
                    store_lines: 1,
                    chunks: 1,
                    ..LokiStatsCounts::default()
                }
                .expected_stats(),
            }
            .json()
    );
}

#[tokio::test]
async fn compactor_delete_requests_filter_querier_tail_results() {
    let delete_requests = SharedLogDeleteRequests::default();
    create_delete_request_with(&block_builder_config(), &delete_requests).await;

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
        wal_group_id: "krabka-observability-querier-tail".to_string(),
        data_root: dir,
        ..minimal_service_config(Role::Querier)
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
    let (mut socket, server) = open_tail(
        app,
        "/loki/api/v1/tail?query=%7Bapp%3D%22api%22%7D&start=0.000000000&end=20000000000",
    )
    .await;
    let frame = next_frame_within_two_seconds(&mut socket).await;
    server.abort();

    assert!(
        frame
            == json!({
                "streams": [
                    {
                        "stream": {
                            "app": "api",
                            "env": "prod"
                        },
                        "values": [
                            ["17000000000", "api later secret"]
                        ]
                    }
                ],
            })
    );
}

#[tokio::test]
async fn compactor_delete_requests_filter_querier_patterns_results() {
    let querier_app = secret_then_public_querier(SecretThenPublic {
        secret_line: "status=500 user=100 secret",
        public_line: "status=200 user=200 public",
    })
    .await;

    let response = Tenant("tenant-a").get(&querier_app, "/loki/api/v1/patterns?query=%7Bapp%3D%22api%22%7D&start=14000000000&end=17000000001&step=1s").await;

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
    let querier_app = secret_then_public_querier(SecretThenPublic {
        secret_line: r#"{"status":"500","secret_field":"hidden","msg":"secret"}"#,
        public_line: r#"{"status":"200","visible_field":"kept","msg":"public"}"#,
    })
    .await;

    let fields_response = Tenant("tenant-a").get(&querier_app, "/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&start=14000000000&end=17000000000&limit=10").await;
    assert!(fields_response.status() == StatusCode::OK);
    assert!(
        json_body(fields_response).await
            == json!({
                "fields": [
                    {
                        "label": "msg",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": ["json"],
                        "jsonPath": ["msg"]
                    },
                    {
                        "label": "status",
                        "type": "int",
                        "cardinality": 1,
                        "parsers": ["json"],
                        "jsonPath": ["status"]
                    },
                    {
                        "label": "visible_field",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": ["json"],
                        "jsonPath": ["visible_field"]
                    }
                ],
                "limit": 10
            })
    );

    let values_response = Tenant("tenant-a").get(&querier_app, "/loki/api/v1/detected_field/status/values?query=%7Bapp%3D%22api%22%7D&start=14000000000&end=17000000000&limit=10").await;
    assert!(values_response.status() == StatusCode::OK);
    assert!(
        json_body(values_response).await
            == json!({
                "values": ["200"],
                "limit": 10
            })
    );
}

/// Lists tenant-a's delete requests and returns them, after checking that the
/// list holds exactly one.
async fn only_tenant_a_delete_request(app: &axum::Router) -> Value {
    let list_response = Tenant("tenant-a").get(app, "/loki/api/v1/delete").await;
    assert!(list_response.status() == StatusCode::OK);
    let body = json_body(list_response).await;
    check!(body.as_array().unwrap().len() == 1);
    body[0].clone()
}

/// Sends one delete-request call as `tenant` and gives back the status and
/// body.
async fn delete_api_call_for_test(
    app: &axum::Router,
    method: &str,
    uri: &str,
    tenant: &str,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("X-Scope-OrgID", tenant)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = text_body(response).await;
    (
        status,
        serde_json::from_str(&body).unwrap_or(Value::String(body)),
    )
}

/// A delete request removes a tenant's logs for good, so the delete API asks
/// the query authorizer on every call. A refused tenant can create no request,
/// read none of its requests, and cancel none. The refusal persists nothing,
/// and another tenant on the same store is not affected.
#[tokio::test]
async fn a_refused_tenant_can_neither_create_nor_list_nor_cancel_delete_requests() {
    let delete_requests = SharedLogDeleteRequests::default();
    let config = test_service_config(Role::BlockBuilder, ".");
    let allowed = build_service_router(
        &config,
        ServiceDependencies::default().with_delete_requests(delete_requests.clone()),
        None,
    )
    .await
    .unwrap();
    let refusing = build_service_router(
        &config,
        ServiceDependencies::default()
            .with_delete_requests(delete_requests.clone())
            .with_query_authorizer(TenantDenyingQueryAuthorizer { denied: "tenant-a" }),
        None,
    )
    .await
    .unwrap();
    let create = "/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D&start=1&end=2";
    let list = "/loki/api/v1/delete";
    let (status, _) = delete_api_call_for_test(&allowed, "POST", create, "tenant-a").await;
    assert!(status == StatusCode::NO_CONTENT);
    let (_, before) = delete_api_call_for_test(&allowed, "GET", list, "tenant-a").await;
    assert!(before.as_array().map(Vec::len) == Some(1));
    let request_id = before[0]["request_id"].as_str().unwrap().to_string();
    let cancel = format!("/loki/api/v1/delete?request_id={request_id}");

    for (method, uri) in [("POST", create), ("GET", list), ("DELETE", cancel.as_str())] {
        let (status, body) = delete_api_call_for_test(&refusing, method, uri, "tenant-a").await;
        check!(status == StatusCode::FORBIDDEN, "{method}");
        assert_loki_error(&body, "forbidden", "tenant read ACL denied");
    }

    let (_, after) = delete_api_call_for_test(&allowed, "GET", list, "tenant-a").await;
    check!(after == before);
    let (status, _) = delete_api_call_for_test(&refusing, "POST", create, "tenant-b").await;
    check!(status == StatusCode::NO_CONTENT);
}

async fn create_secret_delete_request(delete_requests: &SharedLogDeleteRequests) {
    create_delete_request_with(
        &test_service_config(Role::BlockBuilder, "."),
        delete_requests,
    )
    .await;
}

/// The two lines of the `api` block a [`secret_then_public_querier`] serves.
struct SecretThenPublic<'a> {
    /// The line at 14 s, which the delete request covers.
    secret_line: &'a str,
    /// The line at 17 s, which it does not.
    public_line: &'a str,
}

/// A test-config querier whose one `api` block holds `lines`, and that shares
/// a delete request for the "secret" lines between 14 s and 16 s.
async fn secret_then_public_querier(lines: SecretThenPublic<'_>) -> axum::Router {
    let SecretThenPublic {
        secret_line,
        public_line,
    } = lines;
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
            LogRow::new(api, 14_000_000_000, secret_line, BTreeMap::new()),
            LogRow::new(api, 17_000_000_000, public_line, BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(block);
    write_log_index_manifest(&dir, &label_index, &block_index).unwrap();
    build_service_router(
        &test_service_config(Role::Querier, dir),
        ServiceDependencies::default().with_delete_requests(delete_requests),
        None,
    )
    .await
    .unwrap()
}

fn block_builder_config() -> ServiceConfig {
    ServiceConfig {
        index_prefix: Some("observability/logs".to_string()),
        ..minimal_service_config(Role::BlockBuilder)
    }
}

/// Writes a local manifest whose one tenant-a block holds three `api` lines
/// at 14 s, 15 s and 17 s, the last two of which contain "secret". Returns
/// the data root and the block's size in bytes.
fn secret_lines_manifest() -> (std::path::PathBuf, u64) {
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
    (dir, block_bytes)
}

/// The block-builder's router, which serves the delete API, over `data_root`.
async fn compactor_app_over(data_root: std::path::PathBuf) -> axum::Router {
    let config = ServiceConfig {
        data_root,
        ..block_builder_config()
    };
    build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap()
}

/// A querier over [`secret_lines_manifest`] that shares its delete requests
/// with a compactor.
struct SecretLinesQuerier {
    querier_app: axum::Router,
    /// The size of the manifest's one block.
    block_bytes: u64,
}

impl SecretLinesQuerier {
    /// The querier, after tenant-a asked the compactor to delete its "secret"
    /// lines from 14 s to 16 s.
    async fn after_a_secret_delete_request() -> Self {
        let delete_requests = SharedLogDeleteRequests::default();
        create_delete_request_with(&block_builder_config(), &delete_requests).await;

        let (dir, block_bytes) = secret_lines_manifest();
        let querier_app = querier_over(
            dir,
            ServiceDependencies::default().with_delete_requests(delete_requests),
        )
        .await;
        Self {
            querier_app,
            block_bytes,
        }
    }
}

async fn querier_over(
    data_root: std::path::PathBuf,
    dependencies: ServiceDependencies,
) -> axum::Router {
    let config = ServiceConfig {
        wal_group_id: "krabka-observability-querier".to_string(),
        data_root,
        ..minimal_service_config(Role::Querier)
    };
    build_service_router(&config, dependencies, None)
        .await
        .unwrap()
}

/// Checks a forward stream query over [`secret_lines_manifest`] that a delete
/// request for "secret" lines between 14 s and 16 s filtered.
async fn assert_secret_line_filtered(response: axum::response::Response, block_bytes: u64) {
    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body["data"]["result"]
            == json!([
                {
                    "stream": {
                        "app": "api",
                        "env": "prod"
                    },
                    "values": [
                        ["14000000000", "api ok"],
                        ["17000000000", "api later secret"]
                    ]
                }
            ])
    );
    assert!(
        body["data"]["stats"]
            == LokiStatsCounts {
                store_bytes: block_bytes,
                store_lines: 2,
                chunks: 1,
                ..LokiStatsCounts::default()
            }
            .expected_stats()
    );
}

/// Requests, through a compactor built from `config`, the deletion of
/// tenant-a's `api` lines that contain "secret" between 14 s and 16 s.
async fn create_delete_request_with(
    config: &ServiceConfig,
    delete_requests: &SharedLogDeleteRequests,
) {
    let compactor_app = build_service_router(
        config,
        ServiceDependencies::default().with_delete_requests(delete_requests.clone()),
        None,
    )
    .await
    .unwrap();
    let delete_response = Tenant("tenant-a").send(&compactor_app, Method::POST, "/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22secret%22&start=14&end=16").await;
    assert!(delete_response.status() == StatusCode::NO_CONTENT);
}
