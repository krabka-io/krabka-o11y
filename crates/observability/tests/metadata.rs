//! The label, label-value, and series metadata endpoints.

mod support;

use std::collections::{BTreeMap, BTreeSet};

use assert2::assert;
use axum::{body::Body, http::StatusCode};
use krabka_blockstore::{
    BlockDescriptor, BlockKey, LabelIndex, LogBlockIndex as BlockIndex, TimeRange, labels,
};
use krabka_observability::{InMemoryWalSink, LogWalSink, QuerierState, WalLogRecord, loki_router};
use serde_json::json;
use support::{
    DenyingQueryAuthorizer, Tenant, assert_json_ok, assert_loki_error, current_unix_epoch_nanos,
    fixture, json_body, send, tenant_a_post, text_body,
};

/// The shared fixture with one hot-tail `api` record at 20 ns that carries a
/// `level="error"` label.
async fn hot_error_level_app() -> axum::Router {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("level", "error")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    loki_router(fixture().with_hot_tail(hot_tail, 19))
}

/// A querier whose index names an `api` block at 10-19 ns and a `worker`
/// block at 20-29 ns, with an extra `zone` label on `worker`.
fn api_then_zoned_worker_app() -> axum::Router {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker = label_index.insert_series(
        "tenant-a",
        labels([("app", "worker"), ("env", "prod"), ("zone", "east")]),
    );
    let mut block_index = BlockIndex::default();
    block_index.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        BTreeSet::from([api]),
    ));
    block_index.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        BTreeSet::from([worker]),
    ));
    loki_router(QuerierState::new(dir, label_index, block_index))
}

#[tokio::test]
async fn metadata_endpoints_return_loki_parse_error_text_for_invalid_matcher() {
    let paths = [
        "/loki/api/v1/labels?query=%7Bapp%3D",
        "/loki/api/v1/label/app/values?query=%7Bapp%3D",
        "/loki/api/v1/series?match[]=%7Bapp%3D",
    ];

    for path in paths {
        let state = fixture();
        let app = loki_router(state);

        let response = Tenant("tenant-a").get(&app, path).await;

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(
            text_body(response).await
                == "parse error at line 1, col 6: syntax error: unexpected $end, expecting STRING"
        );
    }
}

#[tokio::test]
async fn series_endpoint_returns_loki_error_for_invalid_time_bound() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a")
        .get(&app, "/loki/api/v1/series?start=not-a-number")
        .await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "could not parse 'start' parameter: strconv.ParseInt: parsing \"not-a-number\": invalid syntax"
    );
}

#[tokio::test]
async fn series_endpoint_allows_missing_matcher_parameter_like_loki() {
    for path in ["/loki/api/v1/series", "/api/prom/series"] {
        let dir = tempfile::tempdir().unwrap().keep();
        let state = QuerierState::new(&dir, LabelIndex::default(), BlockIndex::default());
        let app = loki_router(state);

        let response = Tenant("tenant-a").get(&app, path).await;

        assert!(response.status() == StatusCode::OK, "{path}");
        assert!(
            json_body(response).await
                == json!({
                    "status": "success",
                    "data": []
                })
        );
    }
}

#[tokio::test]
async fn metadata_endpoints_reject_loki_query_ranges_over_limit() {
    let paths = [
        "/loki/api/v1/labels?start=0.000000000&end=2595601000000000",
        "/loki/api/v1/label/app/values?start=0.000000000&end=2595601000000000",
        "/loki/api/v1/series?match%5B%5D=%7Bapp%3D%22api%22%7D&start=0.000000000&end=2595601000000000",
    ];

    for path in paths {
        let state = fixture();
        let app = loki_router(state);

        let response = Tenant("tenant-a").get(&app, path).await;

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(
            text_body(response).await
                == "the query time range exceeds the limit (query length: 721h0m1s, limit: 30d1h)"
        );
    }
}

#[tokio::test]
async fn labels_endpoint_returns_tenant_label_names() {
    let state = fixture();
    let app = loki_router(state);

    for path in ["/loki/api/v1/labels", "/loki/api/v1/label"] {
        let response = Tenant("tenant-a").get(&app, path).await;

        assert_json_ok(
            response,
            &json!({
                "status": "success",
                "data": ["app", "env"]
            }),
        )
        .await;
    }
}

#[tokio::test]
async fn empty_metadata_endpoints_return_loki_sparse_success_shapes() {
    let dir = tempfile::tempdir().unwrap().keep();
    let app = loki_router(QuerierState::new(
        dir,
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    for path in [
        "/loki/api/v1/labels",
        "/loki/api/v1/label",
        "/loki/api/v1/label/app/values",
    ] {
        let response = Tenant("tenant-a").get(&app, path).await;

        assert_json_ok(response, &json!({ "status": "success" })).await;
    }

    for path in [
        "/api/prom/label",
        "/api/prom/label/app/values",
        "/loki/api/v1/detected_labels?limit=10",
        "/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&limit=10",
        "/loki/api/v1/detected_field/status/values?query=%7Bapp%3D%22api%22%7D&limit=10",
    ] {
        let response = Tenant("tenant-a").get(&app, path).await;

        assert_json_ok(response, &json!({})).await;
    }
}

#[tokio::test]
async fn metadata_endpoints_hide_loki_detected_level_enrichment() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    label_index.insert_series(
        "tenant-a",
        labels([
            ("app", "api"),
            ("detected_level", "error"),
            ("env", "prod"),
            ("service_name", "api"),
        ]),
    );
    let app = loki_router(QuerierState::new(dir, label_index, BlockIndex::default()));

    let labels_response = Tenant("tenant-a").get(&app, "/loki/api/v1/labels").await;

    assert_json_ok(
        labels_response,
        &json!({
            "status": "success",
            "data": ["app", "env", "service_name"]
        }),
    )
    .await;

    let series_response = Tenant("tenant-a")
        .get(
            &app,
            "/loki/api/v1/series?match%5B%5D=%7Bapp%3D%22api%22%7D",
        )
        .await;

    assert_json_ok(
        series_response,
        &json!({
            "status": "success",
            "data": [
                {
                    "app": "api",
                    "env": "prod",
                    "service_name": "api"
                }
            ]
        }),
    )
    .await;
}

#[tokio::test]
async fn labels_endpoint_includes_hot_wal_tail_label_names() {
    let app = hot_error_level_app().await;

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/labels").await;

    assert_json_ok(
        response,
        &json!({
            "status": "success",
            "data": ["app", "env", "level"]
        }),
    )
    .await;
}

#[tokio::test]
async fn deprecated_api_prom_metadata_endpoints_return_loki_metadata() {
    let state = fixture();
    let app = loki_router(state);

    let label_response = Tenant("tenant-a").get(&app, "/api/prom/label").await;
    assert_json_ok(
        label_response,
        &json!({
            "values": ["app", "env"]
        }),
    )
    .await;

    let values_response = Tenant("tenant-a")
        .get(&app, "/api/prom/label/env/values")
        .await;
    assert_json_ok(
        values_response,
        &json!({
            "values": ["app", "env"]
        }),
    )
    .await;

    let series_response = Tenant("tenant-a")
        .get(
            &app,
            "/api/prom/series?match%5B%5D=%7Bapp%3D%22api%22%7D&start=0.000000000&end=0.000000030",
        )
        .await;
    assert_json_ok(
        series_response,
        &json!({
            "status": "success",
            "data": [
                {
                    "app": "api",
                    "env": "prod"
                }
            ]
        }),
    )
    .await;

    let series_post_response = send(
        &app,
        tenant_a_post("/api/prom/series")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(
                "match%5B%5D=%7Bapp%3D%22api%22%7D&start=0.000000000&end=0.000000030",
            ))
            .unwrap(),
    )
    .await;
    assert_json_ok(
        series_post_response,
        &json!({
            "status": "success",
            "data": [
                {
                    "app": "api",
                    "env": "prod"
                }
            ]
        }),
    )
    .await;
}

#[tokio::test]
async fn labels_endpoint_applies_time_range() {
    let app = api_then_zoned_worker_app();

    let response = Tenant("tenant-a")
        .get(
            &app,
            "/loki/api/v1/labels?start=0.000000010&end=0.000000019",
        )
        .await;

    assert_json_ok(
        response,
        &json!({
            "status": "success",
            "data": ["app", "env"]
        }),
    )
    .await;
}

#[tokio::test]
async fn label_values_endpoint_applies_since_when_start_is_absent() {
    let app = api_then_zoned_worker_app();

    let response = Tenant("tenant-a")
        .get(
            &app,
            "/loki/api/v1/label/app/values?end=0.000000029&since=9ns",
        )
        .await;

    assert_json_ok(
        response,
        &json!({
            "status": "success",
            "data": ["worker"]
        }),
    )
    .await;
}

#[tokio::test]
async fn labels_endpoint_applies_since_with_default_end() {
    let dir = tempfile::tempdir().unwrap().keep();
    let now_ns = i64::try_from(current_unix_epoch_nanos()).unwrap();
    let recent_range = TimeRange::new(now_ns - 1_000_000_000, now_ns - 1_000_000_000).unwrap();
    let old_range = TimeRange::new(now_ns - 600_000_000_000, now_ns - 600_000_000_000).unwrap();
    let mut label_index = LabelIndex::default();
    let recent = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "prod"), ("zone", "west")]),
    );
    let old = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "prod"), ("legacy", "true")]),
    );
    let mut block_index = BlockIndex::default();
    block_index.insert(BlockDescriptor::new(
        BlockKey::new(
            "tenant-a",
            0,
            recent_range.start_ns,
            recent_range.end_ns,
            recent_range,
        ),
        BTreeSet::from([recent]),
    ));
    block_index.insert(BlockDescriptor::new(
        BlockKey::new(
            "tenant-a",
            0,
            old_range.start_ns,
            old_range.end_ns,
            old_range,
        ),
        BTreeSet::from([old]),
    ));
    let app = loki_router(QuerierState::new(dir, label_index, block_index));

    let response = Tenant("tenant-a")
        .get(&app, "/loki/api/v1/labels?since=5m")
        .await;

    assert_json_ok(
        response,
        &json!({
            "status": "success",
            "data": ["app", "env", "zone"]
        }),
    )
    .await;
}

#[tokio::test]
async fn labels_endpoint_applies_selector_query() {
    let app = api_then_zoned_worker_app();

    let response = Tenant("tenant-a")
        .get(&app, "/loki/api/v1/labels?query=%7Bapp%3D%22api%22%7D")
        .await;

    assert_json_ok(
        response,
        &json!({
            "status": "success",
            "data": ["app", "env"]
        }),
    )
    .await;
}

#[tokio::test]
async fn label_metadata_endpoints_accept_form_encoded_post_body() {
    let state = fixture();
    let app = loki_router(state);

    let labels_response = send(
        &app,
        tenant_a_post("/loki/api/v1/labels")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from("query=%7Bapp%3D%22api%22%7D"))
            .unwrap(),
    )
    .await;

    assert_json_ok(
        labels_response,
        &json!({
            "status": "success",
            "data": ["app", "env"]
        }),
    )
    .await;

    let values_response = send(
        &app,
        tenant_a_post("/loki/api/v1/label/app/values")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from("query=%7Bapp%3D%22worker%22%7D"))
            .unwrap(),
    )
    .await;

    assert_json_ok(
        values_response,
        &json!({
            "status": "success",
            "data": ["worker"]
        }),
    )
    .await;
}

#[tokio::test]
async fn label_values_endpoint_returns_tenant_values() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a")
        .get(&app, "/loki/api/v1/label/app/values")
        .await;

    assert_json_ok(
        response,
        &json!({
            "status": "success",
            "data": ["api", "worker"]
        }),
    )
    .await;
}

#[tokio::test]
async fn label_values_endpoint_includes_hot_wal_tail_values() {
    let app = hot_error_level_app().await;

    let response = Tenant("tenant-a")
        .get(&app, "/loki/api/v1/label/level/values")
        .await;

    assert_json_ok(
        response,
        &json!({
            "status": "success",
            "data": ["error"]
        }),
    )
    .await;
}

#[tokio::test]
async fn label_values_endpoint_applies_selector_query() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a")
        .get(
            &app,
            "/loki/api/v1/label/app/values?query=%7Bapp%3D%22worker%22%7D",
        )
        .await;

    assert_json_ok(
        response,
        &json!({
            "status": "success",
            "data": ["worker"]
        }),
    )
    .await;
}

#[tokio::test]
async fn label_values_endpoint_applies_time_range() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a")
        .get(
            &app,
            "/loki/api/v1/label/app/values?start=0.000000010&end=0.000000019",
        )
        .await;

    assert_json_ok(
        response,
        &json!({
            "status": "success",
            "data": ["api"]
        }),
    )
    .await;
}

#[tokio::test]
async fn series_endpoint_applies_matchers_time_range_and_tenant() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/series?match%5B%5D=%7Benv%3D%22prod%22%7D&start=0.000000020&end=0.000000030").await;

    assert_json_ok(
        response,
        &json!({
            "status": "success",
            "data": [
                {
                    "app": "worker",
                    "env": "prod"
                }
            ]
        }),
    )
    .await;
}

#[tokio::test]
async fn label_names_endpoint_rejects_unauthorized_tenant_read() {
    let state = fixture().with_query_authorizer(DenyingQueryAuthorizer);
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/labels").await;

    assert!(response.status() == StatusCode::FORBIDDEN);
    assert_loki_error(
        &json_body(response).await,
        "forbidden",
        "tenant read ACL denied",
    );
}

#[tokio::test]
async fn series_endpoint_includes_matching_hot_wal_tail_series() {
    let app = hot_error_level_app().await;

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/series?match%5B%5D=%7Blevel%3D%22error%22%7D&start=0.000000000&end=0.000000030").await;

    assert_json_ok(
        response,
        &json!({
            "status": "success",
            "data": [
                {
                    "app": "api",
                    "level": "error"
                }
            ]
        }),
    )
    .await;
}

#[tokio::test]
async fn series_endpoint_accepts_form_encoded_post_body() {
    let state = fixture();
    let app = loki_router(state);

    let response = send(
        &app,
        tenant_a_post("/loki/api/v1/series")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(
                "match%5B%5D=%7Benv%3D%22prod%22%7D&start=0.000000020&end=0.000000030",
            ))
            .unwrap(),
    )
    .await;

    assert_json_ok(
        response,
        &json!({
            "status": "success",
            "data": [
                {
                    "app": "worker",
                    "env": "prod"
                }
            ]
        }),
    )
    .await;
}

#[tokio::test]
async fn series_endpoint_accepts_post_query_parameters_when_body_is_empty() {
    let state = fixture();
    let app = loki_router(state);

    let response = send(&app, tenant_a_post("/loki/api/v1/series?match%5B%5D=%7Bapp%3D%22worker%22%7D&start=0.000000020&end=0.000000030").body(Body::empty()).unwrap()).await;

    assert_json_ok(
        response,
        &json!({
            "status": "success",
            "data": [
                {
                    "app": "worker",
                    "env": "prod"
                }
            ]
        }),
    )
    .await;
}

#[tokio::test]
async fn series_endpoint_merges_post_query_parameters_with_form_body() {
    let state = fixture();
    let app = loki_router(state);

    let response = send(
        &app,
        tenant_a_post("/loki/api/v1/series?start=0.000000020&end=0.000000030")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from("match%5B%5D=%7Benv%3D%22prod%22%7D"))
            .unwrap(),
    )
    .await;

    assert_json_ok(
        response,
        &json!({
            "status": "success",
            "data": [
                {
                    "app": "worker",
                    "env": "prod"
                }
            ]
        }),
    )
    .await;
}

#[tokio::test]
async fn series_endpoint_accepts_form_post_matcher_with_raw_ampersand() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api&edge"), ("level", "info")]),
            timestamp_ns: 20,
            line: "api edge hot".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);

    let response = send(
        &app,
        tenant_a_post("/loki/api/v1/series")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(
                r#"match[]={app="api&edge"}&start=0.000000000&end=0.000000030"#,
            ))
            .unwrap(),
    )
    .await;

    assert_json_ok(
        response,
        &json!({
            "status": "success",
            "data": [
                {
                    "app": "api&edge",
                    "level": "info"
                }
            ]
        }),
    )
    .await;
}
