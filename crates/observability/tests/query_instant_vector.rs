//! The instant query endpoint over vector expressions.

mod support;

use assert2::assert;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use krabka_observability::loki_router;
use serde_json::{Value, json};
use support::{
    LokiSuccess, Tenant, assert_json_ok, expected_loki_stats, fixture, json_body, text_body,
};
use tower::ServiceExt as _;

// The answer `tenant-a` gets from the fixture for `uri`: a success of
// `result_type` that carries `data_result`.
struct TenantAnswer<'a> {
    uri: &'a str,
    result_type: &'static str,
    data_result: Value,
}

impl TenantAnswer<'_> {
    async fn assert_served(self) {
        let app = loki_router(fixture());
        let response = Tenant("tenant-a").get(&app, self.uri).await;
        assert_json_ok(
            response,
            &LokiSuccess {
                result_type: self.result_type,
                data_result: self.data_result,
                stats: expected_loki_stats(),
            }
            .json(),
        )
        .await;
    }
}

#[tokio::test]
async fn query_endpoint_matches_lokis_one_digit_time_forwarding() {
    let response = loki_router(fixture())
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=vector%281%29&time=1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await["data"]["result"][0]["value"][0] == json!(1_000_000_000i64));
}

#[tokio::test]
async fn query_endpoint_accepts_grafana_loki_health_vector_expression() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=vector%281%29%2Bvector%281%29&time=4000000000000000000",
        result_type: "vector",
        data_result: json!([
            {
                "metric": {},
                "value": [4_000_000_000i64, "2"]
            }
        ]),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_accepts_vector_function_expression() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=vector%281.5%29&time=4000000000000000000",
        result_type: "vector",
        data_result: json!([
            {
                "metric": {},
                "value": [4_000_000_000i64, "1.5"]
            }
        ]),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_accepts_scalar_arithmetic_expression() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=1%2B1&time=4000000000000000000",
        result_type: "scalar",
        data_result: json!([4_000_000_000_i64, "2"]),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_accepts_scientific_vector_function_expression() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=vector%282.5e-1%29&time=4000000000000000000",
        result_type: "vector",
        data_result: json!([
            {
                "metric": {},
                "value": [4_000_000_000i64, "0.25"]
            }
        ]),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_rejects_signed_vector_function_literals_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    for (query, sign) in [("vector%28-2.5e-1%29", "-"), ("vector%28%2B.5%29", "+")] {
        let response = Tenant("tenant-a")
            .get(
                &app,
                &format!("/loki/api/v1/query?query={query}&time=4000000000000000000"),
            )
            .await;

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(
            text_body(response).await
                == format!(
                    "parse error at line 1, col 8: syntax error: unexpected {sign}, expecting NUMBER"
                )
        );
    }
}

#[tokio::test]
async fn query_endpoint_rejects_unspaced_vector_set_operators_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    for query in [
        "vector%281%29orvector%282%29",
        "vector%281%29andvector%282%29",
    ] {
        let response = Tenant("tenant-a")
            .get(
                &app,
                &format!("/loki/api/v1/query?query={query}&time=4000000000000000000"),
            )
            .await;

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(
            text_body(response).await
                == "parse error at line 1, col 10: syntax error: unexpected IDENTIFIER"
        );
    }
}

#[tokio::test]
async fn query_endpoint_accepts_vector_arithmetic_expression() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=vector%285%29-vector%282%29%2Avector%281.5%29&time=4000000000000000000",
        result_type: "vector",
        data_result: json!([
            {
                "metric": {},
                "value": [4_000_000_000i64, "2"]
            }
        ]),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_accepts_vector_power_and_modulo_expression() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=vector%282%29%5Evector%283%29%2Bvector%285%29%25vector%282%29&time=4000000000000000000",
        result_type: "vector",
        data_result: json!([
            {
                "metric": {},
                "value": [4_000_000_000i64, "9"]
            }
        ]),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_accepts_parenthesized_vector_expression() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=%28vector%281%29%2Bvector%282%29%29%2Avector%283%29&time=4000000000000000000",
        result_type: "vector",
        data_result: json!([
            {
                "metric": {},
                "value": [4_000_000_000i64, "9"]
            }
        ]),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_accepts_vector_literal_arithmetic_expression() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=vector%284%29%2B2&time=4000000000000000000",
        result_type: "vector",
        data_result: json!([
            {
                "metric": {},
                "value": [4_000_000_000i64, "6"]
            }
        ]),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_accepts_vector_and_expression() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=vector%282%29%20and%20vector%281%29&time=4000000000000000000",
        result_type: "vector",
        data_result: json!([
            {
                "metric": {},
                "value": [4_000_000_000i64, "2"]
            }
        ]),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_accepts_vector_or_expression() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=vector%282%29%20or%20vector%281%29&time=4000000000000000000",
        result_type: "vector",
        data_result: json!([
            {
                "metric": {},
                "value": [4_000_000_000i64, "2"]
            }
        ]),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_accepts_vector_unless_expression() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=vector%282%29%20unless%20vector%281%29&time=4000000000000000000",
        result_type: "vector",
        data_result: json!([]),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_accepts_vector_arithmetic_on_modifier() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=vector%286%29%20%2F%20on%28%29%20vector%283%29&time=4000000000000000000",
        result_type: "vector",
        data_result: json!([
            {
                "metric": {},
                "value": [4_000_000_000i64, "2"]
            }
        ]),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_accepts_vector_bool_comparison_ignoring_modifier() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=vector%281%29%20%3E%20bool%20ignoring%28app%29%20vector%282%29&time=4000000000000000000",
        result_type: "vector",
        data_result: json!([
            {
                "metric": {},
                "value": [4_000_000_000i64, "0"]
            }
        ]),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_accepts_vector_group_left_modifier() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=vector%286%29%20%2F%20on%28app%29%20group_left%28status%29%20vector%283%29&time=4000000000000000000",
        result_type: "vector",
        data_result: json!([
            {
                "metric": {},
                "value": [4_000_000_000i64, "2"]
            }
        ]),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_accepts_vector_group_right_modifier() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=vector%282%29%20%3E%20bool%20ignoring%28app%29%20group_right%28zone%29%20vector%281%29&time=4000000000000000000",
        result_type: "vector",
        data_result: json!([
            {
                "metric": {},
                "value": [4_000_000_000i64, "1"]
            }
        ]),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_accepts_label_replace_vector_function() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=label_replace%28vector%281%29%2C%20%22service%22%2C%20%22api-%241%22%2C%20%22missing%22%2C%20%22%28.%2A%29%22%29&time=4000000000000000000",
        result_type: "vector",
        data_result: json!([
            {
                "metric": {
                    "service": "api-"
                },
                "value": [4_000_000_000i64, "1"]
            }
        ]),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_accepts_sort_label_replace_vector_function() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=sort%28label_replace%28vector%281%29%2C%20%22service%22%2C%20%22api-%241%22%2C%20%22missing%22%2C%20%22%28.%2A%29%22%29%29&time=4000000000000000000",
        result_type: "vector",
        data_result: json!([
            {
                "metric": {
                    "service": "api-"
                },
                "value": [4_000_000_000i64, "1"]
            }
        ]),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_accepts_sort_desc_label_replace_vector_function() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=sort_desc%28label_replace%28vector%281%29%2C%20%22service%22%2C%20%22api-%241%22%2C%20%22missing%22%2C%20%22%28.%2A%29%22%29%29&time=4000000000000000000",
        result_type: "vector",
        data_result: json!([
            {
                "metric": {
                    "service": "api-"
                },
                "value": [4_000_000_000i64, "1"]
            }
        ]),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_applies_label_replace_vector_arithmetic_operand() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=label_replace%28vector%281%29%2C%20%22service%22%2C%20%22api-%241%22%2C%20%22missing%22%2C%20%22%28.%2A%29%22%29%20%2B%20on%28%29%20vector%282%29&time=4000000000000000000",
        result_type: "vector",
        data_result: json!([
            {
                "metric": {},
                "value": [4_000_000_000i64, "3"]
            }
        ]),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_orders_label_replace_vector_set_or_like_loki() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=label_replace%28vector%281%29%2C%20%22service%22%2C%20%22api-%241%22%2C%20%22missing%22%2C%20%22%28.%2A%29%22%29%20or%20vector%282%29&time=4000000000000000000",
        result_type: "vector",
        data_result: json!([
            {
                "metric": {},
                "value": [4_000_000_000i64, "2"]
            },
            {
                "metric": {
                    "service": "api-"
                },
                "value": [4_000_000_000i64, "1"]
            }
        ]),
    }
    .assert_served()
    .await;
}

#[tokio::test]
async fn query_endpoint_rejects_label_join_vector_function_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=label_join%28vector%281%29%2C%20%22joined%22%2C%20%22%2F%22%2C%20%22app%22%2C%20%22missing%22%29&time=4000000000000000000").await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = text_body(response).await;
    assert!(body.contains("expecting range aggregation"), "{body}");
}

#[tokio::test]
async fn query_endpoint_rejects_parenthesized_label_join_vector_function_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/query?query=%28label_join%28vector%281%29%2C%20%22joined%22%2C%20%22%2F%22%2C%20%22app%22%2C%20%22missing%22%29%29&time=4000000000000000000").await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = text_body(response).await;
    assert!(body.contains("expecting range aggregation"), "{body}");
}

#[tokio::test]
async fn query_endpoint_rejects_unsupported_scalar_vector_function_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = Tenant("tenant-a")
        .get(
            &app,
            "/loki/api/v1/query?query=abs%28vector%28-1.2%29%29&time=4000000000000000000",
        )
        .await;

    assert!(
        response.status() == StatusCode::BAD_REQUEST,
        "unsupported scalar functions must stay aligned with Loki's parser"
    );
    assert!(
        text_body(response).await
            == "parse error at line 1, col 1: syntax error: unexpected IDENTIFIER"
    );
}

#[tokio::test]
async fn query_endpoint_accepts_vector_filter_comparison_expression() {
    TenantAnswer {
        uri: "/loki/api/v1/query?query=vector%281%29%3Evector%282%29&time=4000000000000000000",
        result_type: "vector",
        data_result: json!([]),
    }
    .assert_served()
    .await;
}
