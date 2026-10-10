//! The range query endpoint, over both stream and matrix results.

mod support;

use std::collections::BTreeMap;

use assert2::{assert, check};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use datafusion::arrow::{
    array::{Float64Array, MapArray, StringArray, TimestampNanosecondArray},
    datatypes::{DataType, TimeUnit},
    record_batch::RecordBatch,
};
use krabka_blockstore::{
    BlockKey, LabelIndex, LogBlockIndex as BlockIndex, LogRow, TimeRange, labels, write_log_block,
};
use krabka_observability::{
    InMemoryWalSink, Limits, LogWalSink, QuerierState, WalLogRecord, loki_router,
};
use krabka_units::{convert::ByteSizeExt as _, nanos};
use serde_json::{Value, json};
use support::{
    LokiStatsCounts, LokiSuccess, Tenant, api_prod_streams, api_prod_wal_record,
    api_seconds_block_app, assert_json_ok, assert_loki_error, check_one_stored_line_stats,
    expected_api_error, expected_loki_stats, fixture, json_body, loki_forwarded_fixture,
    multi_tenant_fixture, parquet_batch, send, tenant_a_post, text_body,
};
use tower::ServiceExt as _;

/// Checks that `batch` holds a UTC nanosecond `timestamp` column, a `labels`
/// column, and a third column named `value_name` of `value_type`, and returns
/// its timestamps.
fn check_parquet_schema<'a>(
    batch: &'a RecordBatch,
    value_name: &str,
    value_type: &DataType,
) -> &'a TimestampNanosecondArray {
    check!(batch.schema().field(0).name() == "timestamp");
    check!(
        batch.schema().field(0).data_type()
            == &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    );
    check!(batch.schema().field(1).name() == "labels");
    check!(batch.schema().field(2).name() == value_name);
    check!(batch.schema().field(2).data_type() == value_type);

    batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap()
}

/// The matrix of the one `{app="api", env="prod"}` series, with `values`.
fn api_prod_matrix(values: &Value) -> Value {
    json!([
        {
            "metric": {
                "app": "api",
                "env": "prod"
            },
            "values": values
        }
    ])
}

/// The matrix of the one `{app="api", env="prod"}` series, with `value` at
/// 30 ns.
fn api_prod_matrix_at_30ns(value: &str) -> Value {
    api_prod_matrix(&json!([[0.000_000_03, value]]))
}

// The answer `tenant-a` gets from the fixture for `uri`: a success of
// `result_type` that carries `data_result` and `stats`.
struct FixtureAnswer<'a> {
    uri: &'a str,
    result_type: &'static str,
    data_result: Value,
    stats: Value,
}

impl FixtureAnswer<'_> {
    async fn assert_served(self) {
        let app = loki_router(fixture());
        let response = Tenant("tenant-a").get(&app, self.uri).await;
        assert_json_ok(
            response,
            &LokiSuccess {
                result_type: self.result_type,
                data_result: self.data_result,
                stats: self.stats,
            }
            .json(),
        )
        .await;
    }
}

#[tokio::test]
async fn deprecated_api_prom_query_range_endpoint_returns_loki_streams_json() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/api/prom/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0.000000000&end=0.000000030").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "streams",
            data_result: api_prod_streams(json!([["19", "api error"]])),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn deprecated_api_prom_query_range_endpoint_rejects_metric_results() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/api/prom/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B5s%5D%29&start=0.000000000&end=0.000000030&step=1s").await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "rpc error: code = Code(400) desc = legacy endpoints only support streams result type"
    );
}

#[tokio::test]
async fn deprecated_api_prom_query_range_endpoint_accepts_form_encoded_post_body() {
    let state = fixture();
    let app = loki_router(state);

    let response = send(&app, tenant_a_post("/api/prom/query_range").header("content-type", "application/x-www-form-urlencoded").body(Body::from("query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0.000000000&end=0.000000030")).unwrap()).await;

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response)
            .await
            .pointer("/data/result/0/values/0/1")
            .and_then(Value::as_str)
            == Some("api error")
    );
}

#[tokio::test]
async fn query_range_endpoint_applies_metric_binary_arithmetic() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%2F%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0.000000000&end=0.000000030",
        result_type: "matrix",
        data_result: api_prod_matrix_at_30ns("2"),
        stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_applies_bool_metric_binary_comparison() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%3C%20bool%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0.000000000&end=0.000000030",
        result_type: "matrix",
        data_result: api_prod_matrix_at_30ns("0"),
        stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_applies_metric_binary_set_or() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%20or%20count_over_time%28%7Bapp%3D%22worker%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0.000000000&end=0.000000030").await;

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/result")
            == Some(&json!([
                {
                    "metric": {
                        "app": "api",

                        "env": "prod"
                    },
                    "values": [
                        [0.000_000_03, "1"]
                    ]
                },
                {
                    "metric": {
                        "app": "worker",

                        "env": "prod"
                    },
                    "values": [
                        [0.000_000_03, "1"]
                    ]
                }
            ]))
    );
}

#[tokio::test]
async fn query_range_endpoint_rejects_approx_topk_metric_query() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query_range?query=approx_topk%282%2C%20count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%29&start=0.000000000&end=0.000000030").await;

    assert!(response.status() == StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        text_body(response).await == "approx_topk is not enabled. See -limits.shard_aggregations"
    );
}

#[tokio::test]
async fn query_range_endpoint_applies_bool_metric_query_scalar_comparison() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%20%3E%20bool%200&start=0.000000000&end=0.000000030",
        result_type: "matrix",
        data_result: api_prod_matrix_at_30ns("1"),
        stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_applies_bool_scalar_metric_query_comparison() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=0%20%3E%20bool%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0.000000000&end=0.000000030",
        result_type: "matrix",
        data_result: api_prod_matrix_at_30ns("0"),
        stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_applies_metric_query_scalar_arithmetic() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%20%2A%202&start=0.000000000&end=0.000000030",
        result_type: "matrix",
        data_result: api_prod_matrix_at_30ns("2"),
        stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_applies_scalar_metric_query_arithmetic() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=2%20%2A%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0.000000000&end=0.000000030",
        result_type: "matrix",
        data_result: api_prod_matrix_at_30ns("2"),
        stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn metric_query_endpoint_populates_loki_stats_from_planned_cold_blocks() {
    let (app, expected_block_bytes) = api_seconds_block_app();

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&time=0.000000019").await;

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    check!(body["data"]["resultType"] == "vector");
    check!(body["data"]["result"][0]["value"][1] == "1");
    check_one_stored_line_stats(&body, expected_block_bytes);
}

#[tokio::test]
async fn metric_query_endpoint_splits_stats_for_cold_blocks_and_hot_tail_samples() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "dev")]),
            timestamp_ns: 20_000_000_000,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = loki_forwarded_fixture().with_hot_tail(hot_tail, 19_000_000_000);
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&time=0.000000030").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([
                {
                    "metric": {
                        "app": "api",

                        "env": "dev"
                    },
                    "value": [30, "1"]
                },
                {
                    "metric": {
                        "app": "api",

                        "env": "prod"
                    },
                    "value": [30, "1"]
                }
            ]),
            stats: LokiStatsCounts {
                store_bytes: 1846,
                store_lines: 1,
                ingester_lines: 1,
                chunks: 1,
            }
            .expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_range_endpoint_applies_start_end_and_tenant() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0.000000000&end=0.000000030").await;

    assert_json_ok(response, &expected_api_error()).await;
}

#[tokio::test]
async fn query_range_endpoint_returns_streams_as_parquet_when_requested() {
    let batch = parquet_batch(loki_router(fixture()), "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0.000000000&end=0.000000030&direction=forward").await;

    assert!(batch.num_rows() == 1);
    let timestamps = check_parquet_schema(&batch, "line", &DataType::Utf8);
    assert!(timestamps.value(0) == 19);
    let labels = batch.column(1).as_any().downcast_ref::<MapArray>().unwrap();
    assert!(labels.value_offsets() == &[0, 2]);
    let keys = labels
        .keys()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let values = labels
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for (index, expected_key, expected_value) in [(0, "app", "api"), (1, "env", "prod")] {
        check!(keys.value(index) == expected_key);
        check!(values.value(index) == expected_value);
    }
    let lines = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(lines.value(0) == "api error");
}

#[tokio::test]
async fn query_range_endpoint_ignores_zero_quality_parquet_accept() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0.000000000&end=0.000000030")
                .header("X-Scope-OrgID", "tenant-a")
                .header(
                    "accept",
                    "application/vnd.apache.parquet;q=0, application/json",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::OK);
    check!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            == Some("application/json")
    );
    check!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn query_range_metric_endpoint_fans_out_pipe_separated_tenant_header() {
    let (state, prod_bytes, stage_bytes) = multi_tenant_fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a|tenant-b").get(&app, "/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30ns%5D%29&start=0.000000029&end=0.000000029&step=1ns").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "matrix",
            data_result: json!([
                {
                    "metric": {
                        "app": "api",

                        "env": "prod"
                    },
                    "values": [
                        [0.000_000_029, "1"]
                    ]
                },
                {
                    "metric": {
                        "app": "api",

                        "env": "stage"
                    },
                    "values": [
                        [0.000_000_029, "1"]
                    ]
                }
            ]),
            stats: LokiStatsCounts {
                store_bytes: prod_bytes + stage_bytes,
                store_lines: 2,
                chunks: 2,
                ..LokiStatsCounts::default()
            }
            .expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_range_endpoint_returns_metrics_as_parquet_when_requested() {
    let batch = parquet_batch(loki_router(fixture()), "/loki/api/v1/query_range?query=2%2Avector%283%29&start=0.000000000&end=0.000000020&step=10ns").await;

    assert!(batch.num_rows() == 3);
    let timestamps = check_parquet_schema(&batch, "value", &DataType::Float64);
    for (index, want) in [(0, 0), (1, 10), (2, 20)] {
        check!(timestamps.value(index) == want);
    }
    let labels = batch.column(1).as_any().downcast_ref::<MapArray>().unwrap();
    assert!(labels.value_offsets() == &[0, 0, 0, 0]);
    let values = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    for index in [0, 1, 2] {
        check!((values.value(index) - 6.0).abs() < f64::EPSILON);
    }
}

#[tokio::test]
async fn query_range_endpoint_logfmt_sanitizes_ansi_prefixed_field_names() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let mut block_index = BlockIndex::default();
    let block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 10, TimeRange::new(10, 10).unwrap()),
        vec![LogRow::new(
            api,
            10,
            "\u{1b}[31mstatus=503 msg=\"colored parser error\"\u{1b}[0m",
            BTreeMap::new(),
        )],
    )
    .unwrap();
    block_index.insert(block);
    let app = loki_router(QuerierState::new(dir, label_index, block_index));

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20line_format%20%60%7B%7B.msg%7D%7D%60&start=0.000000010&end=0.000000011").await;

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    let stream = body.pointer("/data/result/0/stream").unwrap();
    check!(stream.get("_31mstatus") == Some(&json!("503")));
    check!(stream.get("\u{1b}[31mstatus").is_none());
    check!(body.pointer("/data/result/0/values") == Some(&json!([["10", "colored parser error"]])));
}

#[tokio::test]
async fn query_range_endpoint_keep_stage_retains_only_requested_base_labels() {
    let state = fixture();
    let app = loki_router(state);

    let response = send(&app, tenant_a_post("/loki/api/v1/query_range").header("content-type", "application/x-www-form-urlencoded").body(Body::from("query={app=\"api\"} | keep app&start=0.000000000&end=0.000000030&direction=forward")).unwrap()).await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "streams",
            data_result: json!([
                {
                    "stream": {"app": "api"},
                    "values": [
                        ["10", "api ok"],
                        ["19", "api error"]
                    ]
                }
            ]),
            stats: LokiStatsCounts::fixture_block_lines(2).expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_range_endpoint_accepts_rfc3339_time_bounds() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=1970-01-01T00%3A00%3A00Z&end=1970-01-01T00%3A00%3A00.000000030Z").await;

    assert_json_ok(response, &expected_api_error()).await;
}

#[tokio::test]
async fn query_range_endpoint_applies_interval_to_stream_results() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(api_prod_wal_record(20, "api close error"))
        .await
        .unwrap();
    hot_tail
        .append(api_prod_wal_record(29, "api later error"))
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0.000000019&end=0.000000030&direction=forward&interval=10ns").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "streams",
            data_result: api_prod_streams(json!([["19", "api error"], ["29", "api later error"]])),
            stats: LokiStatsCounts {
                store_bytes: 1846,
                store_lines: 1,
                ingester_lines: 1,
                chunks: 1,
            }
            .expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_range_endpoint_excludes_stream_entries_at_end_bound() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(api_prod_wal_record(29, "api boundary error"))
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0.000000019&end=0.000000029&direction=forward").await;

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "api error"]])));
}

#[tokio::test]
async fn query_range_endpoint_accepts_zero_interval_as_noop() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0.000000000&end=0.000000030&direction=forward&interval=0").await;

    assert_json_ok(response, &expected_api_error()).await;
}

#[tokio::test]
async fn query_range_endpoint_returns_loki_error_for_negative_interval() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=0.000000000&end=0.000000030&interval=-1").await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(text_body(response).await == "interval must be >= 0");
}

#[tokio::test]
async fn query_range_endpoint_applies_since_when_start_is_absent() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&end=0.000000030&since=5ns&direction=forward",
        result_type: "streams",
        data_result: json!([]),
        stats: expected_loki_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_uses_default_end_with_since() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&since=2000000000s&direction=forward").await;

    // `since` with no explicit `end` resolves `end = now`, `start = now - since`, so the
    // resolved range equals `since` (= 2_000_000_000s). Real Loki 3.4.2 caps the resolved
    // query range at 30d1h (721h), so this 555555h33m20s window is rejected with 400.
    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "the query time range exceeds the limit (query length: 555555h33m20s, limit: 30d1h)"
    );
}

#[tokio::test]
async fn query_range_endpoint_returns_loki_error_for_invalid_since() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a")
        .get(
            &app,
            "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&end=1.000000000&since=-1",
        )
        .await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "could not parse 'since' parameter: not a valid duration string: \"-1\""
    );
}

#[tokio::test]
async fn query_range_endpoint_defaults_to_recent_range() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22",
        result_type: "streams",
        data_result: json!([]),
        stats: expected_loki_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_returns_count_over_time_matrix_json() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0.000000000&end=0.000000030",
        result_type: "matrix",
        data_result: api_prod_matrix_at_30ns("1"),
        stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_absent_over_time_uses_selector_labels_only() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=absent_over_time%28%7Bapp%3D%22missing%22%2Cenv%3D%22prod%22%7D%5B1ns%5D%29&start=0.000000001&end=0.000000002&step=1ns",
        result_type: "matrix",
        data_result: json!([
            {
                "metric": {
                    "app": "missing",
                    "env": "prod"
                },
                "values": [
                    [0.000_000_001, "1"],
                    [0.000_000_002, "1"]
                ]
            }
        ]),
        stats: expected_loki_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_applies_negative_count_over_time_offset() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B1ns%5D%20offset%20-9ns%29&start=0.000000010&end=0.000000010&step=1ns",
        result_type: "matrix",
        data_result: api_prod_matrix(&json!([[0.000_000_01, "1"]])),
        stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_accepts_range_selector_before_pipeline() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%20%7C%3D%20%22error%22%29&start=0.000000000&end=0.000000030",
        result_type: "matrix",
        data_result: api_prod_matrix_at_30ns("1"),
        stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_rejects_signed_vector_function_literals_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query_range?query=vector%28-2.5e-1%29&start=0.000000000&end=0.000000020&step=10ns").await;

    // Real Loki 3.4.2 rejects a signed literal inside `vector(...)`: a unary `-` is
    // not a NUMBER token, so the LogQL parser errors at column 8 (same as the instant
    // `/query` endpoint — see `query_endpoint_rejects_signed_vector_function_literals_like_loki`).
    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "parse error at line 1, col 8: syntax error: unexpected -, expecting NUMBER"
    );
}

#[tokio::test]
async fn query_range_endpoint_accepts_scalar_expression_as_matrix() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=1%2B2&start=0.000000000&end=0.000000020&step=10ns",
        result_type: "matrix",
        data_result: json!([
            {
                "metric": {},
                "values": [
                    [0, "3"],
                    [0.000_000_01, "3"],
                    [0.000_000_02, "3"]
                ]
            }
        ]),
        stats: expected_loki_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_accepts_vector_arithmetic_expression() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=vector%286%29%2Fvector%284%29&start=0.000000000&end=0.000000020&step=10ns",
        result_type: "matrix",
        data_result: json!([
            {
                "metric": {},
                "values": [
                    [0, "1.5"],
                    [0.000_000_01, "1.5"],
                    [0.000_000_02, "1.5"]
                ]
            }
        ]),
        stats: expected_loki_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_accepts_vector_modulo_expression() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=vector%285%29%25vector%282%29&start=0.000000000&end=0.000000020&step=10ns",
        result_type: "matrix",
        data_result: json!([
            {
                "metric": {},
                "values": [
                    [0, "1"],
                    [0.000_000_01, "1"],
                    [0.000_000_02, "1"]
                ]
            }
        ]),
        stats: expected_loki_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_accepts_parenthesized_vector_expression() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=vector%288%29%2F%28vector%281%29%2Bvector%283%29%29&start=0.000000000&end=0.000000020&step=10ns",
        result_type: "matrix",
        data_result: json!([
            {
                "metric": {},
                "values": [
                    [0, "2"],
                    [0.000_000_01, "2"],
                    [0.000_000_02, "2"]
                ]
            }
        ]),
        stats: expected_loki_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_accepts_literal_vector_arithmetic_expression() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=2%2Avector%283%29&start=0.000000000&end=0.000000020&step=10ns",
        result_type: "matrix",
        data_result: json!([
            {
                "metric": {},
                "values": [
                    [0, "6"],
                    [0.000_000_01, "6"],
                    [0.000_000_02, "6"]
                ]
            }
        ]),
        stats: expected_loki_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_accepts_vector_bool_comparison_expression() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=vector%281%29%3Ebool%20vector%282%29&start=0.000000000&end=0.000000020&step=10ns",
        result_type: "matrix",
        data_result: json!([
            {
                "metric": {},
                "values": [
                    [0, "0"],
                    [0.000_000_01, "0"],
                    [0.000_000_02, "0"]
                ]
            }
        ]),
        stats: expected_loki_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_returns_metric_timestamps_as_unix_seconds_numbers() {
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
            LogRow::new(api, 1_000_000_000, "api first", BTreeMap::new()),
            LogRow::new(api, 2_000_000_000, "api second", BTreeMap::new()),
        ],
    )
    .unwrap();
    let expected_block_bytes = api_block.size.bytes_u64();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let app = loki_router(QuerierState::new(dir, label_index, block_index));

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B1ns%5D%29&start=1.000000000&end=2.000000000&step=1s").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "matrix",
            data_result: json!([
                {
                    "metric": {
                        "app": "api",

                    },
                    "values": [
                        [1, "1"],
                        [2, "1"]
                    ]
                }
            ]),
            stats: LokiStatsCounts {
                store_bytes: expected_block_bytes,
                store_lines: 2,
                chunks: 1,
                ..LokiStatsCounts::default()
            }
            .expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_range_endpoint_accepts_form_encoded_post_body() {
    let state = fixture();
    let app = loki_router(state);

    let response = send(&app, tenant_a_post("/loki/api/v1/query_range").header("content-type", "application/x-www-form-urlencoded").body(Body::from("query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0.000000000&end=0.000000030")).unwrap()).await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "matrix",
            data_result: json!([
                {
                    "metric": {
                        "app": "api",

                        "env": "prod"
                    },
                    "values": [
                        [0.000_000_03, "1"]
                    ]
                }
            ]),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_range_endpoint_includes_loki_stats_object() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0.000000000&end=0.000000030").await;

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/stats")
            .and_then(Value::as_object)
            .is_some()
    );
}

#[tokio::test]
async fn query_range_endpoint_treats_integer_step_as_seconds() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0.000000020&end=10000000020&step=10",
        result_type: "matrix",
        data_result: api_prod_matrix(&json!([[10, "1"], [20, "1"]])),
        stats: LokiStatsCounts::fixture_block_lines(2).expected_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_accepts_float_seconds_step_for_count_over_time_matrix_json() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0.000000020&end=0.000000030&step=0.000000010",
        result_type: "matrix",
        data_result: api_prod_matrix(&json!([[0.000_000_02, "1"], [0.000_000_03, "1"]])),
        stats: LokiStatsCounts::fixture_block_lines(2).expected_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_accepts_duration_step_for_count_over_time_matrix_json() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0.000000020&end=10000000020&step=10s",
        result_type: "matrix",
        data_result: api_prod_matrix(&json!([[10, "1"], [20, "1"]])),
        stats: LokiStatsCounts::fixture_block_lines(2).expected_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_accepts_compound_duration_step_for_grafana() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0.000000020&end=90000000020&step=1m30s",
        result_type: "matrix",
        data_result: json!([
            {
                "metric": {
                    "app": "api",

                    "env": "prod"
                },
                "values": [

                ]
            }
        ]),
        stats: LokiStatsCounts::fixture_block_lines(0).expected_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_accepts_millisecond_duration_step_for_grafana() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0.000000020&end=1.000000020&step=1000ms",
        result_type: "matrix",
        data_result: api_prod_matrix(&json!([[1, "1"], [2, "1"]])),
        stats: LokiStatsCounts::fixture_block_lines(2).expected_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_accepts_compound_duration_range_selector() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B1m30s%5D%29&start=0.000000020&end=0.000000030&step=10ns",
        result_type: "matrix",
        data_result: api_prod_matrix(&json!([[0.000_000_02, "1"], [0.000_000_03, "1"]])),
        stats: LokiStatsCounts::fixture_block_lines(2).expected_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_accepts_trailing_vector_grouping() {
    FixtureAnswer {
        uri: "/loki/api/v1/query_range?query=sum%28count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%29%20by%20%28env%29&start=0.000000000&end=0.000000030",
        result_type: "matrix",
        data_result: json!([
            {
                "metric": {
                    "env": "prod"
                },
                "values": [
                    [0.000_000_03, "1"]
                ]
            }
        ]),
        stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_range_endpoint_returns_loki_error_for_invalid_step() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29&start=0.000000000&end=0.000000030&step=0").await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "zero or negative query resolution step widths are not accepted. Try a positive integer"
    );
}

#[tokio::test]
async fn query_range_endpoint_returns_loki_error_for_invalid_step_duration() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29&start=0.000000000&end=0.000000030&step=not-a-number").await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(text_body(response).await == "cannot parse \"not-a-number\" to a valid duration");
}

#[tokio::test]
async fn query_range_endpoint_returns_loki_error_for_excessive_resolution() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query_range?query=vector%281%29&start=0.000000000&end=11001000000000&step=1s").await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "exceeded maximum resolution of 11,000 points per time series. Try increasing the value of the step parameter"
    );
}

#[tokio::test]
async fn query_range_endpoint_rejects_loki_query_ranges_over_limit() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query_range?query=vector%281%29&start=0.000000000&end=2595601000000000&step=1h").await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "the query time range exceeds the limit (query length: 721h0m1s, limit: 30d1h)"
    );
}

#[tokio::test]
async fn query_range_endpoint_returns_loki_error_for_invalid_start() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=not-a-number&end=1.000000000").await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "could not parse 'start' parameter: strconv.ParseInt: parsing \"not-a-number\": invalid syntax"
    );
}

#[tokio::test]
async fn query_range_endpoint_rejects_ranges_over_configured_limit() {
    let state = fixture().with_limits(Limits {
        max_query_range: nanos(20),
        ..Limits::default()
    });
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=0.000000000&end=0.000000030").await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "query range");
}
