//! The instant query endpoint over metric queries and their binary operators.
//! Fixture blocks contain raw stored rows without ingest-time level metadata.

mod support;

use assert2::{assert, check};
use axum::http::StatusCode;
use datafusion::arrow::array::{Float64Array, MapArray, TimestampNanosecondArray};
use krabka_observability::loki_router;
use serde_json::json;
use support::{
    LokiStatsCounts, LokiSuccess, Tenant, assert_json_ok, assert_loki_error, expected_loki_stats,
    json_body, loki_forwarded_fixture as fixture, parquet_batch, text_body,
};

#[tokio::test]
async fn query_endpoint_returns_metric_query_as_loki_vector_json() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&time=0.000000019").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([
                {
                    "metric": {
                        "app": "api",
                        "env": "prod"
                    },
                    "value": [19, "1"]
                }
            ]),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

/// A synthetic vector sample is written in seconds, as every instant sample
/// is. `time=20000000000` is twenty seconds in nanoseconds.
#[tokio::test]
async fn query_endpoint_returns_synthetic_vector_timestamps_in_seconds() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a")
        .get(
            &app,
            "/loki/api/v1/query?query=2%2Avector%283%29&time=20000000000",
        )
        .await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([
                {
                    "metric": {},
                    "value": [20, "6"]
                }
            ]),
            stats: expected_loki_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_endpoint_returns_vector_metrics_as_parquet_when_requested() {
    let batch = parquet_batch(
        loki_router(fixture()),
        "/loki/api/v1/query?query=2%2Avector%283%29&time=0.000000020",
    )
    .await;

    assert!(batch.num_rows() == 1);
    for (index, name) in [(0, "timestamp"), (1, "labels"), (2, "value")] {
        check!(batch.schema().field(index).name() == name);
    }
    let timestamps = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert!(timestamps.value(0) == 20);
    let labels = batch.column(1).as_any().downcast_ref::<MapArray>().unwrap();
    assert!(labels.value_offsets() == &[0, 0]);
    let values = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((values.value(0) - 6.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn query_endpoint_filters_metric_query_with_scalar_comparison() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%20%3E%201&time=0.000000019").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([]),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_metric_vector_bool_comparison_on_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%20%3E%20bool%20on%28%29%20vector%280%29&time=0.000000019").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([
                {
                    "metric": {},
                    "value": [19, "1"]
                }
            ]),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_metric_vector_set_and_on_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%20and%20on%28%29%20vector%281%29&time=0.000000019").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([
                {
                    "metric": {
                        "app": "api",
                        "env": "prod"
                    },
                    "value": [19, "1"]
                }
            ]),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_vector_metric_set_or_on_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=vector%281%29%20or%20on%28%29%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&time=0.000000019").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([
                {
                    "metric": {},
                    "value": [19, "1"]
                }
            ]),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_metric_query_scalar_arithmetic() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%20%2A%202&time=0.000000019").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([
                {
                    "metric": {
                        "app": "api",
                        "env": "prod"
                    },
                    "value": [19, "2"]
                }
            ]),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_metric_vector_arithmetic_on_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%20%2B%20on%28%29%20vector%281%29&time=0.000000019").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([
                {
                    "metric": {},
                    "value": [19, "2"]
                }
            ]),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_vector_metric_arithmetic_group_right_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=vector%281%29%20%2B%20on%28%29%20group_right%28app%2C%20env%29%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&time=0.000000019").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([
                {
                    "metric": {},
                    "value": [19, "2"]
                }
            ]),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_scalar_metric_query_arithmetic() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=2%20-%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&time=0.000000019").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([
                {
                    "metric": {
                        "app": "api",
                        "env": "prod"
                    },
                    "value": [19, "1"]
                }
            ]),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_parenthesized_metric_query_scalar_arithmetic() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=%28count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%20%2A%202%29&time=0.000000019").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([
                {
                    "metric": {
                        "app": "api",
                        "env": "prod"
                    },
                    "value": [19, "2"]
                }
            ]),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_parenthesized_metric_operand_scalar_arithmetic() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=%28count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%29%20%2A%202&time=0.000000019").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([
                {
                    "metric": {
                        "app": "api",
                        "env": "prod"
                    },
                    "value": [19, "2"]
                }
            ]),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_arithmetic() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%2F%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&time=0.000000019").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([
                {
                    "metric": {
                        "app": "api",
                        "env": "prod"
                    },
                    "value": [19, "2"]
                }
            ]),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_arithmetic_ignoring_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%2F%20ignoring%28app%29%20count_over_time%28%7Bapp%3D%22worker%22%7D%5B30s%5D%29&time=0.000000025").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([
                {
                    "metric": {
                        "app": "api",
                        "env": "prod"
                    },
                    "value": [25, "2"]
                }
            ]),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_arithmetic_group_left_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=sum%20by%28app%2C%20env%29%28count_over_time%28%7Benv%3D%22prod%22%7D%5B30s%5D%29%29%20%2F%20on%28env%29%20group_left%20sum%20by%28env%29%28count_over_time%28%7Benv%3D%22prod%22%7D%5B30s%5D%29%29&time=0.000000025").await;

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
                    "value": [25, "0.666666666"]
                },
                {
                    "metric": {
                        "app": "worker",
                        "env": "prod"
                    },
                    "value": [25, "0.333333333"]
                }
            ]))
    );
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_arithmetic_group_right_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=sum%20by%28env%29%28count_over_time%28%7Benv%3D%22prod%22%7D%5B30s%5D%29%29%20%2F%20on%28env%29%20group_right%20sum%20by%28app%2C%20env%29%28count_over_time%28%7Benv%3D%22prod%22%7D%5B30s%5D%29%29&time=0.000000025").await;

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
                    "value": [25, "1.5"]
                },
                {
                    "metric": {
                        "app": "worker",
                        "env": "prod"
                    },
                    "value": [25, "3"]
                }
            ]))
    );
}

#[tokio::test]
async fn query_endpoint_filters_metric_binary_comparison() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%3E%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&time=0.000000019").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([
                {
                    "metric": {
                        "app": "api",
                        "env": "prod"
                    },
                    "value": [19, "2"]
                }
            ]),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_comparison_on_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%3E%20bool%20on%28env%29%20count_over_time%28%7Bapp%3D%22worker%22%7D%5B30s%5D%29&time=0.000000025").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([
                {
                    "metric": {
                        "app": "api",
                        "env": "prod"
                    },
                    "value": [25, "1"]
                }
            ]),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_comparison_group_left_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=sum%20by%28app%2C%20env%29%28count_over_time%28%7Benv%3D%22prod%22%7D%5B30s%5D%29%29%20%3C%20bool%20on%28env%29%20group_left%20sum%20by%28env%29%28count_over_time%28%7Benv%3D%22prod%22%7D%5B30s%5D%29%29&time=0.000000025").await;

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
                    "value": [25, "1"]
                },
                {
                    "metric": {
                        "app": "worker",
                        "env": "prod"
                    },
                    "value": [25, "1"]
                }
            ]))
    );
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_set_and() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20and%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&time=0.000000019").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([
                {
                    "metric": {
                        "app": "api",
                        "env": "prod"
                    },
                    "value": [19, "2"]
                }
            ]),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_set_on_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20and%20on%28env%29%20count_over_time%28%7Bapp%3D%22worker%22%7D%5B30s%5D%29&time=0.000000025").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([
                {
                    "metric": {
                        "app": "api",
                        "env": "prod"
                    },
                    "value": [25, "2"]
                }
            ]),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_set_unless() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20unless%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&time=0.000000019").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([]),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

#[tokio::test]
async fn query_endpoint_filters_scalar_metric_query_comparison() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=2%20%3E%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&time=0.000000019").await;

    assert_json_ok(
        response,
        &LokiSuccess {
            result_type: "vector",
            data_result: json!([
                {
                    "metric": {
                        "app": "api",
                        "env": "prod"
                    },
                    "value": [19, "1"]
                }
            ]),
            stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
        }
        .json(),
    )
    .await;
}

// The one `{app="api", env="prod"}` sample at 19 ns, relabelled with
// `service="api-api"`, that a `label_replace` query over the fixture answers
// with `value`.
struct ApiServiceSample<'a> {
    uri: &'a str,
    value: &'a str,
}

impl ApiServiceSample<'_> {
    async fn assert_served(self) {
        let app = loki_router(fixture());
        let response = Tenant("tenant-a").get(&app, self.uri).await;
        assert_json_ok(
            response,
            &LokiSuccess {
                result_type: "vector",
                data_result: json!([
                    {
                        "metric": {
                            "app": "api",
                            "env": "prod",
                            "service": "api-api"
                        },
                        "value": [19, self.value]
                    }
                ]),
                stats: LokiStatsCounts::fixture_block_lines(1).expected_stats(),
            }
            .json(),
        )
        .await;
    }
}

#[tokio::test]
async fn query_endpoint_accepts_label_replace_metric_query() {
    ApiServiceSample {
        uri: "/loki/api/v1/query?query=label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29&time=0.000000019",
        value: "1",
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_accepts_parenthesized_label_replace_metric_query() {
    ApiServiceSample {
        uri: "/loki/api/v1/query?query=%28label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29%29&time=0.000000019",
        value: "1",
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_accepts_label_replace_metric_binary_expression() {
    ApiServiceSample {
        uri: "/loki/api/v1/query?query=label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%2F%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29&time=0.000000019",
        value: "2",
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_arithmetic_with_label_replace_operands() {
    ApiServiceSample {
        uri: "/loki/api/v1/query?query=label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29%20%2F%20label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29&time=0.000000019",
        value: "1",
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_arithmetic_with_label_replace_scalar_operands() {
    ApiServiceSample {
        uri: "/loki/api/v1/query?query=label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%2B%201%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29%20%2F%20label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%2B%201%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29&time=0.000000019",
        value: "1",
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_comparison_with_label_replace_operands() {
    ApiServiceSample {
        uri: "/loki/api/v1/query?query=label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29%20%3E%20bool%20label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29&time=0.000000019",
        value: "0",
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_set_with_label_replace_operands() {
    ApiServiceSample {
        uri: "/loki/api/v1/query?query=label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29%20or%20label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29&time=0.000000019",
        value: "2",
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_group_left_with_label_replace_operands() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=label_replace%28sum%20by%28app%2C%20env%29%28count_over_time%28%7Benv%3D%22prod%22%7D%5B30s%5D%29%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29%20%2F%20on%28env%29%20group_left%20label_replace%28sum%20by%28env%29%28count_over_time%28%7Benv%3D%22prod%22%7D%5B30s%5D%29%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29&time=0.000000025").await;

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/result")
            == Some(&json!([
                {
                    "metric": {
                        "app": "api",
                        "env": "prod",
                        "service": "api-api"
                    },
                    "value": [25, "0.666666666"]
                },
                {
                    "metric": {
                        "app": "worker",
                        "env": "prod",
                        "service": "worker-api"
                    },
                    "value": [25, "0.333333333"]
                }
            ]))
    );
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_comparison_group_left_with_label_replace_operands() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=label_replace%28sum%20by%28app%2C%20env%29%28count_over_time%28%7Benv%3D%22prod%22%7D%5B30s%5D%29%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29%20%3C%20bool%20on%28env%29%20group_left%20label_replace%28sum%20by%28env%29%28count_over_time%28%7Benv%3D%22prod%22%7D%5B30s%5D%29%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29&time=0.000000025").await;

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/result")
            == Some(&json!([
                {
                    "metric": {
                        "app": "api",
                        "env": "prod",
                        "service": "api-api"
                    },
                    "value": [25, "1"]
                },
                {
                    "metric": {
                        "app": "worker",
                        "env": "prod",
                        "service": "worker-api"
                    },
                    "value": [25, "1"]
                }
            ]))
    );
}

#[tokio::test]
async fn query_endpoint_rejects_label_join_metric_query_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=label_join%28count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%2C%20%22joined%22%2C%20%22%2F%22%2C%20%22app%22%2C%20%22env%22%2C%20%22missing%22%29&time=0.000000019").await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = text_body(response).await;
    assert!(body.contains("expecting range aggregation"), "{body}");
}

#[tokio::test]
async fn query_endpoint_rejects_parenthesized_label_join_metric_query_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=%28label_join%28count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%2C%20%22joined%22%2C%20%22%2F%22%2C%20%22app%22%2C%20%22env%22%2C%20%22missing%22%29%29&time=0.000000019").await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = text_body(response).await;
    assert!(body.contains("expecting range aggregation"), "{body}");
}

#[tokio::test]
async fn query_endpoint_rejects_metric_pipeline_errors() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%20json%20%5B30s%5D%29&time=0.000000030").await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "JSONParserErr");
}
