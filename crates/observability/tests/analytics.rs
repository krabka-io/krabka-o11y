//! The index stats and volume endpoints, and the patterns and detected-field endpoints.

mod support;

use std::collections::BTreeMap;

use assert2::assert;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use krabka_blockstore::{
    BlockKey, LabelIndex, LogBlockIndex as BlockIndex, LogRow, TimeRange, labels, write_log_block,
};
use krabka_observability::{QuerierState, loki_router};
use krabka_units::convert::ByteSizeExt as _;
use serde_json::json;
use support::{
    assert_loki_error, expected_loki_stats, expected_loki_stats_with, json_body, text_body,
};
use tower::ServiceExt as _;

#[tokio::test]
async fn index_stats_endpoint_returns_stream_chunk_entry_and_byte_counts() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();
    let expected_block_bytes = api_block.size.bytes_u64();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=10&end=19")
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
                "streams": 1,
                "chunks": 1,
                "entries": 2,
                "bytes": expected_block_bytes,
            })
    );
}

#[tokio::test]
async fn index_shards_endpoint_returns_loki_compatible_bounds_and_stats() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 10, "api ok", BTreeMap::new())],
    )
    .unwrap();
    let bytes = block.size.bytes_u64();
    let mut block_index = BlockIndex::default();
    block_index.insert(block);
    let app = loki_router(QuerierState::new(dir, label_index, block_index));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/shards?query=%7Bapp%3D%22api%22%7D&start=10&end=19&targetBytesPerShard=1")
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
                "shards": [{
                    "bounds": {"min": 0, "max": u64::MAX},
                    "stats": {"streams": 1, "chunks": 1, "entries": 0, "bytes": bytes}
                }],
                "statistics": expected_loki_stats(),
                "chunkGroups": null
            })
    );
}

#[tokio::test]
async fn index_stats_endpoint_accepts_form_encoded_post_body() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();
    let expected_block_bytes = api_block.size.bytes_u64();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/index/stats")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("query=%7Bapp%3D%22api%22%7D&start=10&end=19"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "streams": 1,
                "chunks": 1,
                "entries": 2,
                "bytes": expected_block_bytes,
            })
    );
}

#[tokio::test]
async fn index_volume_endpoint_returns_series_vector_bytes() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 19, "api error", BTreeMap::new())],
    )
    .unwrap();
    let expected_block_bytes = api_block.size.bytes_u64();
    let worker_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 1, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(worker, 19, "worker error", BTreeMap::new())],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    block_index.insert(worker_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/volume?query=%7Bapp%3D%22api%22%7D&start=10&end=19")
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
                                "env": "prod"
                            },
                            "value": [19, expected_block_bytes.to_string()]
                        }
                    ],
                    "stats": expected_loki_stats_with(expected_block_bytes, 0, 1)
                }
            })
    );
}

#[tokio::test]
async fn index_volume_range_endpoint_returns_vector_with_target_labels() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 19, "api error", BTreeMap::new())],
    )
    .unwrap();
    let expected_block_bytes = api_block.size.bytes_u64();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/index/volume_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query=%7Bapp%3D%22api%22%7D&start=10&end=30&step=10ns&targetLabels=app",
                ))
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
                                "app": "api"
                            },
                            "value": [30, expected_block_bytes.to_string()]
                        }
                    ],
                    "stats": expected_loki_stats_with(expected_block_bytes, 0, 1)
                }
            })
    );
}

#[tokio::test]
async fn index_volume_range_endpoint_accepts_form_post_query_with_raw_ampersand() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api&edge")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 19, "api edge error", BTreeMap::new())],
    )
    .unwrap();
    let expected_block_bytes = api_block.size.bytes_u64();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/index/volume_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api&edge"}&start=10&end=30&step=10ns"#,
                ))
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
                                "app": "api&edge"
                            },
                            "value": [30, expected_block_bytes.to_string()]
                        }
                    ],
                    "stats": expected_loki_stats_with(expected_block_bytes, 0, 1)
                }
            })
    );
}

#[tokio::test]
async fn index_volume_range_endpoint_returns_vector_without_target_labels() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 19, "api error", BTreeMap::new())],
    )
    .unwrap();
    let expected_block_bytes = api_block.size.bytes_u64();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/volume_range?query=%7Bapp%3D%22api%22%7D&start=10&end=30&step=10ns")
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
                                "env": "prod"
                            },
                            "value": [30, expected_block_bytes.to_string()]
                        }
                    ],
                    "stats": expected_loki_stats_with(expected_block_bytes, 0, 1)
                }
            })
    );
}

#[tokio::test]
async fn index_volume_endpoints_default_missing_start_to_recent_range() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    for endpoint in ["index/volume", "index/volume_range"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/loki/api/v1/{endpoint}?query=%7Bapp%3D%22api%22%7D&end=1000000000&step=1s"
                    ))
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
                        "result": [],
                        "stats": expected_loki_stats()
                    }
                })
        );
    }
}

#[tokio::test]
async fn index_stats_endpoint_requires_start_parameter() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&end=1000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(
        &json_body(response).await,
        "bad_data",
        "missing query parameter `start`",
    );
}

#[tokio::test]
async fn index_volume_endpoints_default_missing_end_to_current_time() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    for endpoint in ["index/volume", "index/volume_range"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/loki/api/v1/{endpoint}?query=%7Bapp%3D%22api%22%7D&start=0"
                    ))
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(
            text_body(response)
                .await
                .starts_with("the query time range exceeds the limit (query length: ")
        );
    }
}

#[tokio::test]
async fn index_stats_endpoint_requires_end_parameter() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=0")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(
        &json_body(response).await,
        "bad_data",
        "missing query parameter `end`",
    );
}

#[tokio::test]
async fn index_stats_endpoint_rejects_loki_query_ranges_over_limit() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=0&end=2595601000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "the query time range exceeds the limit (query length: 721h0m1s, limit: 30d1h)"
    );
}

#[tokio::test]
async fn index_volume_range_endpoint_returns_loki_error_for_zero_step() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/volume_range?query=%7Bapp%3D%22api%22%7D&start=0&end=1000000000&step=0")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    assert!(
        std::str::from_utf8(&body).unwrap()
            == "zero or negative query resolution step widths are not accepted. Try a positive integer"
    );
}

#[tokio::test]
async fn index_volume_endpoint_returns_loki_error_for_invalid_aggregate_by() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/volume?query=%7Bapp%3D%22api%22%7D&start=0&end=1000000000&aggregateBy=bogus")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    assert!(std::str::from_utf8(&body).unwrap() == "invalid aggregation option");
}

#[tokio::test]
async fn index_endpoints_return_loki_error_for_invalid_logql() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    for endpoint in ["index/stats", "index/volume", "index/volume_range"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/loki/api/v1/{endpoint}?query=%7Bapp%3D&start=0&end=1"
                    ))
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(
            text_body(response).await
                == "parse error at line 1, col 6: syntax error: unexpected $end, expecting STRING"
        );
    }
}

#[tokio::test]
async fn index_volume_endpoint_supports_label_aggregation_and_limit() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 19, "api error", BTreeMap::new())],
    )
    .unwrap();
    let expected_block_bytes = api_block.size.bytes_u64();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/volume?query=%7Bapp%3D%22api%22%7D&start=10&end=19&aggregateBy=labels&targetLabels=app,env&limit=1")
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
                                "app": ""
                            },
                            "value": [19, expected_block_bytes.to_string()]
                        }
                    ],
                    "stats": expected_loki_stats_with(expected_block_bytes, 0, 1)
                }
            })
    );
}

#[tokio::test]
async fn patterns_endpoint_groups_matching_logs_by_detected_pattern() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let worker = label_index.insert_series("tenant-a", labels([("app", "worker")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            100_000_000,
            1_100_000_000,
            TimeRange::new(100_000_000, 1_100_000_000).unwrap(),
        ),
        vec![
            LogRow::new(
                api,
                100_000_000,
                "ts=2024-03-30T23:03:40 caller=grpc_logging.go:66 level=info method=/cortex.Ingester/Push duration=200ms msg=gRPC",
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                1_100_000_000,
                "ts=2024-03-30T23:03:41 caller=grpc_logging.go:66 level=info method=/cortex.Ingester/Push duration=500ms msg=gRPC",
                BTreeMap::new(),
            ),
            LogRow::new(
                worker,
                1_100_000_000,
                "ts=2024-03-30T23:03:41 caller=worker.go:10 level=info duration=5ms msg=ignored",
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/patterns?query=%7Bapp%3D%22api%22%7D&start=0&end=2000000000&step=1s")
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
                        "pattern": "ts=<_> caller=grpc_logging.go:66 level=info method=/cortex.Ingester/Push duration=<_> msg=gRPC",
                        "samples": [
                            [0, 1],
                            [1, 1]
                        ]
                    }
                ]
            })
    );
}

#[tokio::test]
async fn patterns_endpoint_collapses_json_logs_differing_only_by_timestamp() {
    // Krabka services emit compact JSON logs whose only per-line-varying field is
    // the timestamp. The Patterns tab must mine these five lines into a single
    // `<_>`-templated pattern rather than one row per line (the bug this guards).
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let broker = label_index.insert_series("tenant-a", labels([("service_name", "broker")]));
    let rows = (0i64..5)
        .map(|i| {
            let ts = 100_000_000 + i * 100_000_000;
            let line = format!(
                r#"{{"timestamp":"2026-07-01T04:19:2{i}.1238077Z","severity":"INFO","target":"krabka_broker::network::dispatch","message":"connection opened"}}"#
            );
            LogRow::new(broker, ts, line, BTreeMap::new())
        })
        .collect::<Vec<_>>();
    let block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            100_000_000,
            600_000_000,
            TimeRange::new(100_000_000, 600_000_000).unwrap(),
        ),
        rows,
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/patterns?query=%7Bservice_name%3D%22broker%22%7D&start=0&end=2000000000&step=1s")
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
                        "pattern": r#"{"timestamp":"<_>","severity":"INFO","target":"krabka_broker::network::dispatch","message":"connection opened"}"#,
                        "samples": [
                            [0, 5]
                        ]
                    }
                ]
            })
    );
}

#[tokio::test]
async fn patterns_endpoint_excludes_entries_at_end_bound() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            1_000_000_000,
            2_000_000_000,
            TimeRange::new(1_000_000_000, 2_000_000_000).unwrap(),
        ),
        vec![
            LogRow::new(
                api,
                1_000_000_000,
                "status=500 user=100 route=/checkout",
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                2_000_000_000,
                "status=200 user=200 route=/checkout",
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/patterns?query=%7Bapp%3D%22api%22%7D&start=0&end=2000000000&step=1s")
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
                        "pattern": "status=<_> user=<_> route=/checkout",
                        "samples": [
                            [1, 1]
                        ]
                    }
                ]
            })
    );
}

#[tokio::test]
async fn patterns_endpoint_accepts_form_encoded_post_body() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            100_000_000,
            1_100_000_000,
            TimeRange::new(100_000_000, 1_100_000_000).unwrap(),
        ),
        vec![
            LogRow::new(
                api,
                100_000_000,
                "status=500 user=100 route=/checkout",
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                1_100_000_000,
                "status=200 user=200 route=/checkout",
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/patterns")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query=%7Bapp%3D%22api%22%7D&start=0&end=2000000000&step=1s",
                ))
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
                        "pattern": "status=<_> user=<_> route=/checkout",
                        "samples": [
                            [0, 1],
                            [1, 1]
                        ]
                    }
                ]
            })
    );
}

#[tokio::test]
async fn patterns_endpoint_accepts_form_post_query_with_raw_ampersand() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api&edge")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            100_000_000,
            1_100_000_000,
            TimeRange::new(100_000_000, 1_100_000_000).unwrap(),
        ),
        vec![
            LogRow::new(
                api,
                100_000_000,
                "status=500 user=100 route=/checkout",
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                1_100_000_000,
                "status=200 user=200 route=/checkout",
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/patterns")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api&edge"}&start=0&end=2000000000&step=1s"#,
                ))
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
                        "pattern": "status=<_> user=<_> route=/checkout",
                        "samples": [
                            [0, 1],
                            [1, 1]
                        ]
                    }
                ]
            })
    );
}

#[tokio::test]
async fn patterns_endpoint_returns_loki_error_for_invalid_logql() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/patterns?query=%7Bapp%3D&start=0&end=1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "parse error at line 1, col 6: syntax error: unexpected $end, expecting STRING"
    );
}

#[tokio::test]
async fn detected_fields_stops_scanning_at_the_line_limit() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(10, 20).unwrap()),
        vec![
            LogRow::new(
                api,
                10,
                r#"{"status":500,"ok":false,"path":"/checkout"}"#,
                BTreeMap::from([("trace_id".to_string(), "abc".to_string())]),
            ),
            LogRow::new(
                api,
                11,
                "level=warn duration=12ms bytes=1.5MiB status=503",
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&start=10&end=20&limit=10&line_limit=1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    // Only the first row is scanned, so the second row's logfmt fields --
    // `duration`, `bytes`, and its own `status` and `level` -- never appear.
    assert!(
        json_body(response).await
            == json!({
                "fields": [
                    {
                        "label": "detected_level",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": null
                    },
                    {
                        "label": "ok",
                        "type": "boolean",
                        "cardinality": 1,
                        "parsers": ["json"]
                    },
                    {
                        "label": "path",
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
                        "label": "trace_id",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": ["structured_metadata"]
                    }
                ],
                "limit": 10
            })
    );
}

#[tokio::test]
async fn detected_fields_endpoint_discovers_json_logfmt_and_structured_metadata() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(10, 20).unwrap()),
        vec![
            LogRow::new(
                api,
                10,
                r#"{"status":500,"ok":false,"path":"/checkout"}"#,
                BTreeMap::from([("trace_id".to_string(), "abc".to_string())]),
            ),
            LogRow::new(
                api,
                11,
                "level=warn duration=12ms bytes=1.5MiB status=503",
                BTreeMap::new(),
            ),
            LogRow::new(
                worker,
                12,
                r#"{"status":200,"worker_field":"ignored"}"#,
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&start=10&end=20&limit=10")
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
                "fields": [
                    {
                        "label": "bytes",
                        "type": "bytes",
                        "cardinality": 1,
                        "parsers": ["logfmt"]
                    },
                    {
                        "label": "detected_level",
                        "type": "string",
                        "cardinality": 2,
                        "parsers": null
                    },
                    {
                        "label": "duration",
                        "type": "duration",
                        "cardinality": 1,
                        "parsers": ["logfmt"]
                    },
                    {
                        "label": "level",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": ["logfmt"]
                    },
                    {
                        "label": "ok",
                        "type": "boolean",
                        "cardinality": 1,
                        "parsers": ["json"]
                    },
                    {
                        "label": "path",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": ["json"]
                    },
                    {
                        "label": "status",
                        "type": "int",
                        "cardinality": 2,
                        "parsers": ["json", "logfmt"]
                    },
                    {
                        "label": "trace_id",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": ["structured_metadata"]
                    }
                ],
                "limit": 10
            })
    );
}

#[tokio::test]
async fn detected_labels_endpoint_reports_stream_label_cardinality() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api_prod = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_stage =
        label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "stage")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    let block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(10, 20).unwrap()),
        vec![
            LogRow::new(api_prod, 10, "api prod", BTreeMap::new()),
            LogRow::new(api_stage, 11, "api stage", BTreeMap::new()),
            LogRow::new(worker, 12, "worker ignored", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_labels?query=%7Bapp%3D%22api%22%7D&start=10&end=20&limit=10")
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
                "detectedLabels": [
                    {
                        "label": "app",
                        "cardinality": 1
                    },
                    {
                        "label": "env",
                        "cardinality": 2
                    }
                ]
            })
    );
}

#[tokio::test]
async fn detected_labels_endpoint_returns_empty_object_without_matches() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(10, 20).unwrap()),
        vec![LogRow::new(api, 10, "api prod", BTreeMap::new())],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_labels?query=%7Bapp%3D%22missing%22%7D&start=10&end=20")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == json!({}));
}

#[tokio::test]
async fn detected_labels_endpoint_defaults_missing_query_to_all_streams() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api_prod = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    let block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(10, 20).unwrap()),
        vec![
            LogRow::new(api_prod, 10, "api prod", BTreeMap::new()),
            LogRow::new(worker, 11, "worker prod", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_labels?start=10&end=20&limit=10")
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
                "detectedLabels": [
                    {
                        "label": "app",
                        "cardinality": 2
                    },
                    {
                        "label": "env",
                        "cardinality": 1
                    }
                ]
            })
    );
}

#[tokio::test]
async fn detected_labels_endpoint_ignores_malformed_step_and_limit_like_loki() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api_prod = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_stage =
        label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "stage")]));
    let block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(10, 20).unwrap()),
        vec![
            LogRow::new(api_prod, 10, "api prod", BTreeMap::new()),
            LogRow::new(api_stage, 11, "api stage", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_labels?query=%7Bapp%3D%22api%22%7D&start=10&end=20&step=not-a-duration&limit=not-a-limit")
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
                "detectedLabels": [
                    {
                        "label": "app",
                        "cardinality": 1
                    },
                    {
                        "label": "env",
                        "cardinality": 2
                    }
                ]
            })
    );
}

#[tokio::test]
async fn detected_field_values_endpoint_accepts_form_post_body() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(10, 20).unwrap()),
        vec![
            LogRow::new(api, 10, r#"{"status":500}"#, BTreeMap::new()),
            LogRow::new(api, 11, "status=503", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/detected_field/status/values")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query=%7Bapp%3D%22api%22%7D&start=10&end=20&limit=1",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "values": ["500"],
                "limit": 1
            })
    );
}

#[tokio::test]
async fn detected_field_values_endpoint_accepts_form_post_query_with_raw_ampersand() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api&edge")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(10, 20).unwrap()),
        vec![
            LogRow::new(api, 10, r#"{"status":500}"#, BTreeMap::new()),
            LogRow::new(api, 11, "status=503", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/detected_field/status/values")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api&edge"}&start=10&end=20&limit=1"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "values": ["500"],
                "limit": 1
            })
    );
}

#[tokio::test]
async fn detected_fields_endpoint_derives_start_from_since_when_start_is_omitted() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(10, 20).unwrap()),
        vec![
            LogRow::new(api, 10, r#"{"old_field":"ignored"}"#, BTreeMap::new()),
            LogRow::new(api, 20, r#"{"new_field":"kept"}"#, BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&end=20&since=5ns")
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
                "fields": [
                    {
                        "label": "detected_level",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": null
                    },
                    {
                        "label": "new_field",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": ["json"]
                    }
                ],
                "limit": 1000
            })
    );
}

#[tokio::test]
async fn detected_field_values_endpoint_accepts_step_duration_parameter() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(10, 20).unwrap()),
        vec![LogRow::new(api, 20, r#"{"status":"200"}"#, BTreeMap::new())],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_field/status/values?query=%7Bapp%3D%22api%22%7D&end=20&since=1m&step=30s")
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
                "values": ["200"],
                "limit": 1000
            })
    );
}

#[tokio::test]
async fn detected_fields_endpoint_rejects_invalid_step_parameter() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&step=not-a-duration")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response)
            .await
            .contains("cannot parse \"not-a-duration\" to a valid duration")
    );
}

#[tokio::test]
async fn detected_fields_endpoint_returns_loki_error_for_zero_step() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&step=0")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "zero or negative query resolution step widths are not accepted. Try a positive integer"
    );
}

#[tokio::test]
async fn detected_fields_endpoint_returns_loki_error_for_invalid_logql() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_fields?query=%7Bapp%3D")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "parse error at line 1, col 6: syntax error: unexpected $end, expecting STRING"
    );
}

#[tokio::test]
async fn detected_fields_endpoint_rejects_loki_query_ranges_over_limit() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&start=0&end=2595601000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "the query time range exceeds the limit (query length: 721h0m1s, limit: 30d1h)"
    );
}

#[tokio::test]
async fn detected_labels_endpoint_rejects_loki_query_ranges_over_limit() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_labels?start=0&end=2595601000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "the query time range exceeds the limit (query length: 721h0m1s, limit: 30d1h)"
    );
}

#[tokio::test]
async fn detected_field_values_endpoint_rejects_loki_query_ranges_over_limit() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_field/status/values?query=%7Bapp%3D%22api%22%7D&start=0&end=2595601000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "the query time range exceeds the limit (query length: 721h0m1s, limit: 30d1h)"
    );
}
