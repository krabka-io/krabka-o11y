//! The index stats and volume endpoints, and the patterns and detected-field endpoints.

mod support;

use std::collections::BTreeMap;

use assert2::{assert, check};
use axum::{Router, body::to_bytes, http::StatusCode};
use krabka_blockstore::{
    BlockKey, LabelIndex, LogBlockIndex as BlockIndex, LogLabels, LogRow, TimeRange, labels,
    write_log_block,
};
use krabka_observability::{
    InMemoryWalSink, LogWalSink as _, QuerierState, WalLogRecord, loki_router,
};
use krabka_units::convert::ByteSizeExt as _;
use serde_json::json;
use support::{
    BlockSpan, LogEntry, LokiStatsCounts, LokiSuccess, Tenant, assert_loki_error,
    expected_loki_stats, json_body, log_entry, post_form, text_body,
};

/// A querier over one tenant-a block at offsets 10-19 that holds `entries`
/// of the series `series_labels`. Returns the router and the block's size in
/// bytes.
fn one_block_app(series_labels: LogLabels, entries: &[LogEntry<'_>]) -> (Router, u64) {
    block_app(
        series_labels,
        BlockSpan {
            first: 10,
            last: 19,
        },
        entries,
    )
}

/// A querier over one tenant-a block at `span` that holds `entries` of the
/// series `series_labels`. Returns the router and the block's size in bytes.
fn block_app(series_labels: LogLabels, span: BlockSpan, entries: &[LogEntry<'_>]) -> (Router, u64) {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let series = label_index.insert_series("tenant-a", series_labels);
    let block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            span.first,
            span.last,
            TimeRange::new(span.first, span.last).unwrap(),
        ),
        entries
            .iter()
            .map(|entry| LogRow::new(series, entry.timestamp_ns, entry.line, BTreeMap::new()))
            .collect(),
    )
    .unwrap();
    let bytes = block.size.bytes_u64();
    let mut block_index = BlockIndex::default();
    block_index.insert(block);
    (
        loki_router(QuerierState::new(dir, label_index, block_index)),
        bytes,
    )
}

fn api_and_worker_series() -> (LabelIndex, u64, u64) {
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    (label_index, api, worker)
}

/// The `api` rows the detected-fields tests scan: a JSON line that carries a
/// `trace_id` in structured metadata, then a logfmt line.
fn detected_field_rows(api: u64) -> Vec<LogRow> {
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
    ]
}

/// A querier over one tenant-a block at offsets 10-20 that holds `rows`.
fn block_rows_app(label_index: LabelIndex, rows: Vec<LogRow>) -> Router {
    let dir = tempfile::tempdir().unwrap().keep();
    let block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(10, 20).unwrap()),
        rows,
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(block);
    loki_router(QuerierState::new(dir, label_index, block_index))
}

#[tokio::test]
async fn index_stats_endpoint_returns_stream_chunk_entry_and_byte_counts() {
    let (app, expected_block_bytes) = one_block_app(
        labels([("app", "api"), ("env", "prod")]),
        &[log_entry(10, "api ok"), log_entry(19, "api error")],
    );

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000019").await;

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
    let (app, bytes) = one_block_app(labels([("app", "api")]), &[log_entry(10, "api ok")]);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/index/shards?query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000019&targetBytesPerShard=1").await;

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
    let (app, expected_block_bytes) = one_block_app(
        labels([("app", "api"), ("env", "prod")]),
        &[log_entry(10, "api ok"), log_entry(19, "api error")],
    );

    let response = post_form(
        &app,
        "/loki/api/v1/index/stats",
        "query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000019",
    )
    .await;

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
    let (label_index, api, worker) = api_and_worker_series();
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

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/index/volume?query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000019").await;

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
                        "value": [0.000_000_019, expected_block_bytes.to_string()]
                    }
                ]),
                stats: LokiStatsCounts {
                    store_bytes: expected_block_bytes,
                    store_lines: 0,
                    chunks: 1,
                    ..LokiStatsCounts::default()
                }
                .expected_stats(),
            }
            .json()
    );
}

#[tokio::test]
async fn index_volume_range_endpoint_returns_matrix_with_target_labels() {
    let (app, expected_block_bytes) = one_block_app(
        labels([("app", "api"), ("env", "prod")]),
        &[log_entry(19, "api error")],
    );

    let response = post_form(
        &app,
        "/loki/api/v1/index/volume_range",
        "query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000030&step=10ns&targetLabels=app",
    )
    .await;

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == LokiSuccess {
                result_type: "matrix",
                data_result: json!([
                    {
                        "metric": {
                            "app": "api"
                        },
                        "values": [[0.000_000_01, expected_block_bytes.to_string()]]
                    }
                ]),
                stats: LokiStatsCounts {
                    store_bytes: expected_block_bytes,
                    store_lines: 0,
                    chunks: 1,
                    ..LokiStatsCounts::default()
                }
                .expected_stats(),
            }
            .json()
    );
}

#[tokio::test]
async fn index_volume_range_endpoint_accepts_form_post_query_with_raw_ampersand() {
    let (app, expected_block_bytes) = one_block_app(
        labels([("app", "api&edge")]),
        &[log_entry(19, "api edge error")],
    );

    let response = post_form(
        &app,
        "/loki/api/v1/index/volume_range",
        r#"query={app="api&edge"}&start=0.000000010&end=0.000000030&step=10ns"#,
    )
    .await;

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == LokiSuccess {
                result_type: "matrix",
                data_result: json!([
                    {
                        "metric": {
                            "app": "api&edge"
                        },
                        "values": [[0.000_000_01, expected_block_bytes.to_string()]]
                    }
                ]),
                stats: LokiStatsCounts {
                    store_bytes: expected_block_bytes,
                    store_lines: 0,
                    chunks: 1,
                    ..LokiStatsCounts::default()
                }
                .expected_stats(),
            }
            .json()
    );
}

#[tokio::test]
async fn index_volume_range_endpoint_returns_matrix_without_target_labels() {
    let (app, expected_block_bytes) = one_block_app(
        labels([("app", "api"), ("env", "prod")]),
        &[log_entry(19, "api error")],
    );

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/index/volume_range?query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000030&step=10ns").await;

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == LokiSuccess {
                result_type: "matrix",
                data_result: json!([
                    {
                        "metric": {
                            "app": "api",
                            "env": "prod"
                        },
                        "values": [[0.000_000_01, expected_block_bytes.to_string()]]
                    }
                ]),
                stats: LokiStatsCounts {
                    store_bytes: expected_block_bytes,
                    store_lines: 0,
                    chunks: 1,
                    ..LokiStatsCounts::default()
                }
                .expected_stats(),
            }
            .json()
    );
}

#[tokio::test]
async fn index_volume_endpoints_default_missing_start_to_recent_range() {
    let app = empty_querier_app();

    for endpoint in ["index/volume", "index/volume_range"] {
        let response = Tenant("tenant-a")
            .get(
                &app,
                &format!("/loki/api/v1/{endpoint}?query=%7Bapp%3D%22api%22%7D&end=1.0&step=1s"),
            )
            .await;

        assert!(response.status() == StatusCode::OK);
        assert!(
            json_body(response).await
                == LokiSuccess {
                    result_type: "vector",
                    data_result: json!([]),
                    stats: expected_loki_stats(),
                }
                .json()
        );
    }
}

#[tokio::test]
async fn index_stats_endpoint_requires_start_parameter() {
    let app = empty_querier_app();

    let response = Tenant("tenant-a")
        .get(
            &app,
            "/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&end=1.0",
        )
        .await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(
        &json_body(response).await,
        "bad_data",
        "missing query parameter `start`",
    );
}

#[tokio::test]
async fn index_volume_endpoints_default_missing_end_to_current_time() {
    let app = empty_querier_app();

    for endpoint in ["index/volume", "index/volume_range"] {
        let response = Tenant("tenant-a")
            .get(
                &app,
                &format!("/loki/api/v1/{endpoint}?query=%7Bapp%3D%22api%22%7D&start=0.000000000"),
            )
            .await;

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
    let app = empty_querier_app();

    let response = Tenant("tenant-a")
        .get(
            &app,
            "/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=0.000000000",
        )
        .await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(
        &json_body(response).await,
        "bad_data",
        "missing query parameter `end`",
    );
}

#[tokio::test]
async fn index_stats_endpoint_rejects_loki_query_ranges_over_limit() {
    let app = empty_querier_app();

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=0.000000000&end=2595601000000000").await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "the query time range exceeds the limit (query length: 721h0m1s, limit: 30d1h)"
    );
}

#[tokio::test]
async fn index_volume_range_endpoint_returns_loki_error_for_zero_step() {
    let app = empty_querier_app();

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/index/volume_range?query=%7Bapp%3D%22api%22%7D&start=0.000000000&end=1.0&step=0").await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    assert!(
        std::str::from_utf8(&body).unwrap()
            == "zero or negative query resolution step widths are not accepted. Try a positive integer"
    );
}

#[tokio::test]
async fn index_volume_endpoint_returns_loki_error_for_invalid_aggregate_by() {
    let app = empty_querier_app();

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/index/volume?query=%7Bapp%3D%22api%22%7D&start=0.000000000&end=1.0&aggregateBy=bogus").await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    assert!(std::str::from_utf8(&body).unwrap() == "invalid aggregation option");
}

#[tokio::test]
async fn index_endpoints_return_loki_error_for_invalid_logql() {
    let app = empty_querier_app();

    for endpoint in ["index/stats", "index/volume", "index/volume_range"] {
        let response = Tenant("tenant-a")
            .get(
                &app,
                &format!(
                    "/loki/api/v1/{endpoint}?query=%7Bapp%3D&start=0.000000000&end=0.000000001"
                ),
            )
            .await;

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(
            text_body(response).await
                == "parse error at line 1, col 6: syntax error: unexpected $end, expecting STRING"
        );
    }
}

#[tokio::test]
async fn index_volume_endpoint_supports_label_aggregation_and_limit() {
    let (app, expected_block_bytes) = one_block_app(
        labels([("app", "api"), ("env", "prod")]),
        &[log_entry(19, "api error")],
    );

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/index/volume?query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000019&aggregateBy=labels&targetLabels=app,env&limit=1").await;

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == LokiSuccess {
                result_type: "vector",
                data_result: json!([
                    {
                        "metric": {
                            "app": ""
                        },
                        "value": [0.000_000_019, expected_block_bytes.to_string()]
                    }
                ]),
                stats: LokiStatsCounts {
                    store_bytes: expected_block_bytes,
                    store_lines: 0,
                    chunks: 1,
                    ..LokiStatsCounts::default()
                }
                .expected_stats(),
            }
            .json()
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

    let response = Tenant("tenant-a")
        .get(
            &app,
            "/loki/api/v1/patterns?query=%7Bapp%3D%22api%22%7D&start=0.000000000&end=2.0&step=1s",
        )
        .await;

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

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/patterns?query=%7Bservice_name%3D%22broker%22%7D&start=0.000000000&end=2.0&step=1s").await;

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
    let (app, _) = block_app(
        labels([("app", "api")]),
        BlockSpan {
            first: 1_000_000_000,
            last: 2_000_000_000,
        },
        &[
            log_entry(1_000_000_000, "status=500 user=100 route=/checkout"),
            log_entry(2_000_000_000, "status=200 user=200 route=/checkout"),
        ],
    );

    let response = Tenant("tenant-a")
        .get(
            &app,
            "/loki/api/v1/patterns?query=%7Bapp%3D%22api%22%7D&start=0.000000000&end=2.0&step=1s",
        )
        .await;

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
    let app = two_checkout_lines_app(labels([("app", "api")]));

    let response = post_form(
        &app,
        "/loki/api/v1/patterns",
        "query=%7Bapp%3D%22api%22%7D&start=0.000000000&end=2.0&step=1s",
    )
    .await;

    assert_one_checkout_pattern(response).await;
}

#[tokio::test]
async fn patterns_endpoint_accepts_form_post_query_with_raw_ampersand() {
    let app = two_checkout_lines_app(labels([("app", "api&edge")]));

    let response = post_form(
        &app,
        "/loki/api/v1/patterns",
        r#"query={app="api&edge"}&start=0.000000000&end=2.0&step=1s"#,
    )
    .await;

    assert_one_checkout_pattern(response).await;
}

#[tokio::test]
async fn patterns_endpoint_returns_loki_error_for_invalid_logql() {
    let app = empty_querier_app();

    let response = Tenant("tenant-a")
        .get(
            &app,
            "/loki/api/v1/patterns?query=%7Bapp%3D&start=0.000000000&end=0.000000001",
        )
        .await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "parse error at line 1, col 6: syntax error: unexpected $end, expecting STRING"
    );
}

#[tokio::test]
async fn detected_fields_stops_scanning_at_the_line_limit() {
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let app = block_rows_app(label_index, detected_field_rows(api));

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000020&limit=10&line_limit=1").await;

    assert!(response.status() == StatusCode::OK);
    // Only the first row is scanned, so the second row's logfmt fields --
    // `duration`, `bytes`, and its own `status` and `level` -- never appear.
    assert!(
        json_body(response).await
            == json!({
                "fields": [

                    {
                        "label": "ok",
                        "type": "boolean",
                        "cardinality": 1,
                        "parsers": ["json"],
                        "jsonPath": ["ok"]
                    },
                    {
                        "label": "path",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": ["json"],
                        "jsonPath": ["path"]
                    },
                    {
                        "label": "status",
                        "type": "int",
                        "cardinality": 1,
                        "parsers": ["json"],
                        "jsonPath": ["status"]
                    },
                    {
                        "label": "trace_id",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": null
                    }
                ],
                "limit": 10
            })
    );
}

#[tokio::test]
async fn detected_fields_endpoint_discovers_json_logfmt_and_structured_metadata() {
    let (label_index, api, worker) = api_and_worker_series();
    let mut rows = detected_field_rows(api);
    rows.push(LogRow::new(
        worker,
        12,
        r#"{"status":200,"worker_field":"ignored"}"#,
        BTreeMap::new(),
    ));
    let app = block_rows_app(label_index, rows);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000020&limit=10").await;

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
                        "parsers": ["json"],
                        "jsonPath": ["ok"]
                    },
                    {
                        "label": "path",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": ["json"],
                        "jsonPath": ["path"]
                    },
                    {
                        "label": "status",
                        "type": "int",
                        "cardinality": 2,
                        "parsers": ["json", "logfmt"],
                        "jsonPath": ["status"]
                    },
                    {
                        "label": "trace_id",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": null
                    }
                ],
                "limit": 10
            })
    );
}

#[tokio::test]
async fn detected_labels_endpoint_reports_stream_label_cardinality() {
    let mut label_index = LabelIndex::default();
    let api_prod = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_stage =
        label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "stage")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    let app = block_rows_app(
        label_index,
        vec![
            LogRow::new(api_prod, 10, "api prod", BTreeMap::new()),
            LogRow::new(api_stage, 11, "api stage", BTreeMap::new()),
            LogRow::new(worker, 12, "worker ignored", BTreeMap::new()),
        ],
    );

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/detected_labels?query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000020&limit=10").await;

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await == AppAndEnvCardinalities { app: 1, env: 2 }.detected_labels()
    );
}

#[tokio::test]
async fn detected_labels_endpoint_returns_empty_object_without_matches() {
    let (app, _) = block_app(
        labels([("app", "api"), ("env", "prod")]),
        BlockSpan {
            first: 10,
            last: 20,
        },
        &[log_entry(10, "api prod")],
    );

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/detected_labels?query=%7Bapp%3D%22missing%22%7D&start=0.000000010&end=0.000000020").await;

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == json!({}));
}

#[tokio::test]
async fn detected_labels_endpoint_defaults_missing_query_to_all_streams() {
    let mut label_index = LabelIndex::default();
    let api_prod = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    let app = block_rows_app(
        label_index,
        vec![
            LogRow::new(api_prod, 10, "api prod", BTreeMap::new()),
            LogRow::new(worker, 11, "worker prod", BTreeMap::new()),
        ],
    );

    let response = Tenant("tenant-a")
        .get(
            &app,
            "/loki/api/v1/detected_labels?start=0.000000010&end=0.000000020&limit=10",
        )
        .await;

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await == AppAndEnvCardinalities { app: 2, env: 1 }.detected_labels()
    );
}

#[tokio::test]
async fn detected_labels_endpoint_ignores_malformed_step_and_limit_like_loki() {
    let mut label_index = LabelIndex::default();
    let api_prod = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_stage =
        label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "stage")]));
    let app = block_rows_app(
        label_index,
        vec![
            LogRow::new(api_prod, 10, "api prod", BTreeMap::new()),
            LogRow::new(api_stage, 11, "api stage", BTreeMap::new()),
        ],
    );

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/detected_labels?query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000020&step=not-a-duration&limit=not-a-limit").await;

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await == AppAndEnvCardinalities { app: 1, env: 2 }.detected_labels()
    );
}

#[tokio::test]
async fn detected_field_values_endpoint_accepts_form_post_body() {
    let (app, _) = block_app(
        labels([("app", "api")]),
        BlockSpan {
            first: 10,
            last: 20,
        },
        &[
            log_entry(10, r#"{"status":500}"#),
            log_entry(11, "status=503"),
        ],
    );

    let response = post_form(
        &app,
        "/loki/api/v1/detected_field/status/values",
        "query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000020&limit=1",
    )
    .await;

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
    let (app, _) = block_app(
        labels([("app", "api&edge")]),
        BlockSpan {
            first: 10,
            last: 20,
        },
        &[
            log_entry(10, r#"{"status":500}"#),
            log_entry(11, "status=503"),
        ],
    );

    let response = post_form(
        &app,
        "/loki/api/v1/detected_field/status/values",
        r#"query={app="api&edge"}&start=0.000000010&end=0.000000020&limit=1"#,
    )
    .await;

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
    let (app, _) = block_app(
        labels([("app", "api")]),
        BlockSpan {
            first: 10,
            last: 20,
        },
        &[
            log_entry(10, r#"{"old_field":"ignored"}"#),
            log_entry(20, r#"{"new_field":"kept"}"#),
        ],
    );

    let response = Tenant("tenant-a")
        .get(
            &app,
            "/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&end=0.000000020&since=5ns",
        )
        .await;

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "fields": [

                    {
                        "label": "new_field",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": ["json"],
                        "jsonPath": ["new_field"]
                    }
                ],
                "limit": 1000
            })
    );
}

#[tokio::test]
async fn detected_field_values_endpoint_accepts_step_duration_parameter() {
    let (app, _) = block_app(
        labels([("app", "api")]),
        BlockSpan {
            first: 10,
            last: 20,
        },
        &[log_entry(20, r#"{"status":"200"}"#)],
    );

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/detected_field/status/values?query=%7Bapp%3D%22api%22%7D&end=0.000000020&since=1m&step=30s").await;

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
    let app = empty_querier_app();

    let response = Tenant("tenant-a")
        .get(
            &app,
            "/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&step=not-a-duration",
        )
        .await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response)
            .await
            .contains("cannot parse \"not-a-duration\" to a valid duration")
    );
}

#[tokio::test]
async fn detected_fields_endpoint_returns_loki_error_for_zero_step() {
    let app = empty_querier_app();

    let response = Tenant("tenant-a")
        .get(
            &app,
            "/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&step=0",
        )
        .await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "zero or negative query resolution step widths are not accepted. Try a positive integer"
    );
}

#[tokio::test]
async fn detected_fields_endpoint_returns_loki_error_for_invalid_logql() {
    let app = empty_querier_app();

    let response = Tenant("tenant-a")
        .get(&app, "/loki/api/v1/detected_fields?query=%7Bapp%3D")
        .await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "parse error at line 1, col 6: syntax error: unexpected $end, expecting STRING"
    );
}

#[tokio::test]
async fn detected_fields_endpoint_rejects_loki_query_ranges_over_limit() {
    let app = empty_querier_app();

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&start=0.000000000&end=2595601000000000").await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "the query time range exceeds the limit (query length: 721h0m1s, limit: 30d1h)"
    );
}

#[tokio::test]
async fn detected_labels_endpoint_rejects_loki_query_ranges_over_limit() {
    let app = empty_querier_app();

    let response = Tenant("tenant-a")
        .get(
            &app,
            "/loki/api/v1/detected_labels?start=0.000000000&end=2595601000000000",
        )
        .await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "the query time range exceeds the limit (query length: 721h0m1s, limit: 30d1h)"
    );
}

#[tokio::test]
async fn detected_field_values_endpoint_rejects_loki_query_ranges_over_limit() {
    let app = empty_querier_app();

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/detected_field/status/values?query=%7Bapp%3D%22api%22%7D&start=0.000000000&end=2595601000000000").await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "the query time range exceeds the limit (query length: 721h0m1s, limit: 30d1h)"
    );
}

/// Entries the block builder has not written yet are still in the WAL. Loki
/// answers for the matching entries from its ingesters, so the detected-field
/// and volume endpoints read the querier's hot tail as well as its blocks.
#[tokio::test]
async fn analytics_endpoints_read_entries_still_in_the_hot_tail() {
    let hot_tail = InMemoryWalSink::default();
    for (timestamp_ns, line, level) in [
        (20, r#"{"status":500}"#, "error"),
        (21, r#"{"status":200}"#, "info"),
    ] {
        hot_tail
            .append(WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([("app", "api"), ("detected_level", level)]),
                timestamp_ns,
                line: line.to_string(),
                structured_metadata: BTreeMap::from([("pod".to_string(), "api-1".to_string())]),
                position: None,
            })
            .await
            .unwrap();
    }
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    )
    .with_hot_tail(hot_tail, i64::MIN);
    let app = loki_router(state);

    let cases = [
        (
            "/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000030&limit=10",
            json!({
                "fields": [

                    {"label": "pod", "type": "string", "cardinality": 1, "parsers": null},
                    {
                        "label": "status",
                        "type": "int",
                        "cardinality": 2,
                        "parsers": ["json"],
                        "jsonPath": ["status"]
                    }
                ],
                "limit": 10
            }),
        ),
        (
            "/loki/api/v1/detected_field/status/values?query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000030",
            json!({"values": ["200", "500"], "limit": 1000}),
        ),
        (
            "/loki/api/v1/index/volume?query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000030&targetLabels=app",
            LokiSuccess {
                result_type: "vector",
                data_result: json!([{"metric": {"app": "api"}, "value": [0.000_000_03, "28"]}]),
                stats: expected_loki_stats(),
            }
            .json(),
        ),
        (
            "/loki/api/v1/index/volume_range?query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000030&step=10ns&targetLabels=app",
            LokiSuccess {
                result_type: "matrix",
                data_result: json!([{"metric": {"app": "api"}, "values": [[0.000_000_02, "28"]]}]),
                stats: expected_loki_stats(),
            }
            .json(),
        ),
    ];
    for (uri, expected) in cases {
        let response = Tenant("tenant-a").get(&app, uri).await;
        check!(response.status() == StatusCode::OK, "{uri}");
        check!(json_body(response).await == expected, "{uri}");
    }
}

/// A querier with no blocks and no series, over a fresh data root.
fn empty_querier_app() -> Router {
    loki_router(QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    ))
}

/// A querier over one tenant-a block of `stream` that holds two `/checkout`
/// lines, at 0.1 s and 1.1 s.
fn two_checkout_lines_app(stream: LogLabels) -> Router {
    block_app(
        stream,
        BlockSpan {
            first: 100_000_000,
            last: 1_100_000_000,
        },
        &[
            log_entry(100_000_000, "status=500 user=100 route=/checkout"),
            log_entry(1_100_000_000, "status=200 user=200 route=/checkout"),
        ],
    )
    .0
}

/// Checks that a `patterns` response over [`two_checkout_lines_app`] finds
/// the one `/checkout` pattern, once in each second.
async fn assert_one_checkout_pattern(response: axum::response::Response) {
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

/// How many values the `app` and `env` labels take in a `detected_labels`
/// answer.
struct AppAndEnvCardinalities {
    app: u64,
    env: u64,
}

impl AppAndEnvCardinalities {
    fn detected_labels(&self) -> serde_json::Value {
        json!({
            "detectedLabels": [
                {
                    "label": "app",
                    "cardinality": self.app
                },
                {
                    "label": "env",
                    "cardinality": self.env
                }
            ]
        })
    }
}
