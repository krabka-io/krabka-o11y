//! The instant query endpoint over stream results.

mod support;

use std::collections::BTreeMap;

use assert2::{assert, check};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use krabka_blockstore::{
    BlockKey, LabelIndex, LogBlockIndex as BlockIndex, LogRow, TimeRange, labels, write_log_block,
};
use krabka_observability::{
    CompactionFrontier, InMemoryWalSink, Limits, LogWalSink, Offset, PartitionIndex, QuerierState,
    SharedCompactionFrontier, WalLogRecord, WalPosition, loki_router,
};
use krabka_units::{bytes, convert::ByteSizeExt as _};
use serde_json::{Value, json};
use support::{
    DenyingQueryAuthorizer, assert_loki_error, expected_api_error, expected_loki_mixed_stats_with,
    expected_loki_stats_with, fixture, json_body, multi_tenant_fixture, text_body,
};
use tower::ServiceExt as _;

#[tokio::test]
async fn query_endpoint_returns_loki_streams_json_for_tenant() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn query_endpoint_fans_out_pipe_separated_tenant_header() {
    let (state, prod_bytes, stage_bytes) = multi_tenant_fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=29",
                )
                .header("X-Scope-OrgID", "tenant-a|tenant-b")
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
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["29", "tenant-a api error"]
                            ]
                        },
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "stage"
                            },
                            "values": [
                                ["29", "tenant-b api error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(
                        prod_bytes + stage_bytes,
                        2,
                        2
                    )
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_form_encoded_post_body() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn deprecated_api_prom_query_endpoint_returns_loki_streams_json() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/prom/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn deprecated_api_prom_query_endpoint_accepts_form_encoded_post_body() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/prom/query")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn deprecated_api_prom_query_endpoint_rejects_metric_results_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/prom/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B5s%5D%29&time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "rpc error: code = Code(400) desc = legacy endpoints only support streams result type"
    );
}

#[tokio::test]
async fn query_endpoint_accepts_fractional_unix_seconds_time() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=0.000000019",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn query_endpoint_includes_loki_stats_object() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/stats")
            .and_then(Value::as_object)
            .is_some()
    );
}

#[tokio::test]
async fn query_endpoint_populates_loki_stats_from_planned_cold_blocks() {
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
    let app = loki_router(QuerierState::new(dir, label_index, block_index));

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    check!(body["data"]["stats"]["store"]["compressedBytes"] == expected_block_bytes);
    check!(body["data"]["stats"]["store"]["decompressedBytes"] == expected_block_bytes);
    check!(body["data"]["stats"]["store"]["decompressedLines"] == 1);
    check!(body["data"]["stats"]["store"]["totalChunksRef"] == 1);
    check!(body["data"]["stats"]["store"]["totalChunksDownloaded"] == 1);
    check!(body["data"]["stats"]["summary"]["totalBytesProcessed"] == expected_block_bytes);
    check!(body["data"]["stats"]["summary"]["totalLinesProcessed"] == 1);
}

#[tokio::test]
async fn query_endpoint_merges_cold_blocks_with_hot_wal_tail() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 19,
            line: "api error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30&direction=forward",
                )
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
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["19", "api error"],
                                ["20", "api hot error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_mixed_stats_with(1819, 1, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_uses_updated_shared_compaction_frontier_for_hot_tail() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: Some(WalPosition {
                partition: PartitionIndex(0),
                offset: Offset(43),
            }),
        })
        .await
        .unwrap();
    let frontier = SharedCompactionFrontier::new(CompactionFrontier::new(0));
    let state = fixture().with_hot_tail_shared_frontier(hot_tail, frontier.clone());
    frontier.advance_partition_offset(WalPosition {
        partition: PartitionIndex(0),
        offset: Offset(43),
    });
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30",
                )
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
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_applies_limit_to_stream_results() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30&direction=forward&limit=1",
                )
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
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_applies_backward_direction_before_limit() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30&direction=backward&limit=1",
                )
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
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["20", "api hot error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_mixed_stats_with(1819, 0, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_defaults_to_backward_direction_before_limit() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30&limit=1",
                )
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
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["20", "api hot error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_mixed_stats_with(1819, 0, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_rejects_invalid_direction() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D&direction=sideways")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(text_body(response).await == "invalid direction 'sideways'");
}

#[tokio::test]
async fn query_endpoint_rejects_missing_tenant_header_as_loki_does() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::UNAUTHORIZED);
    assert!(text_body(response).await == "no org id\n");
}

#[tokio::test]
async fn query_endpoint_rejects_unauthorized_tenant_read() {
    let state = fixture().with_query_authorizer(DenyingQueryAuthorizer);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::FORBIDDEN);
    assert_loki_error(
        &json_body(response).await,
        "forbidden",
        "tenant read ACL denied",
    );
}

#[tokio::test]
async fn query_endpoint_returns_loki_error_for_invalid_logql() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%7Bapp%3D")
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
async fn endpoints_return_loki_error_for_missing_query() {
    let state = fixture();
    let app = loki_router(state);

    for uri in [
        "/loki/api/v1/query",
        "/loki/api/v1/query_range?start=0&end=1",
        "/loki/api/v1/index/stats?start=0&end=1",
        "/loki/api/v1/index/volume?start=0&end=1",
        "/loki/api/v1/index/volume_range?start=0&end=1&step=1ns",
        "/loki/api/v1/detected_fields?start=0&end=1",
        "/loki/api/v1/detected_field/status/values?start=0&end=1",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(text_body(response).await == "parse error : syntax error: unexpected $end");
    }
}

#[tokio::test]
async fn query_endpoint_returns_loki_error_for_invalid_limit() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D&limit=not-a-number")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(text_body(response).await == "strconv.Atoi: parsing \"not-a-number\": invalid syntax");
}

#[tokio::test]
async fn query_endpoint_returns_loki_error_for_negative_limit() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D&limit=-1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(text_body(response).await == "limit must be a positive value");
}

#[tokio::test]
async fn query_endpoint_rejects_series_over_configured_limit() {
    let state = fixture().with_limits(Limits {
        max_query_series: 1,
        ..Limits::default()
    });
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%7Benv%3D%22prod%22%7D&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "series");
}

#[tokio::test]
async fn query_endpoint_rejects_planned_block_bytes_over_configured_limit() {
    let state = fixture().with_limits(Limits {
        max_query_read: bytes(1),
        ..Limits::default()
    });
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "bytes");
}
