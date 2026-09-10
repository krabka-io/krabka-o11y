//! The `line_format` template helpers, over the range query endpoint.

mod support;

use assert2::assert;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use krabka_observability::loki_router;
use serde_json::{Value, json};
use support::{current_unix_epoch_nanos, fixture, json_body};
use tower::ServiceExt as _;

#[tokio::test]
async fn query_range_endpoint_line_format_can_reference_log_timestamp() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%7C%20line_format%20%60%7B%7B%20__timestamp__%20%7C%20unixEpochNanos%20%7D%7D%60&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "19"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_accepts_line_and_timestamp_aliases() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%7C%20line_format%20%60%7B%7B%20line%20%7D%7D%20%7B%7B%20timestamp%20%7C%20unixEpochNanos%20%7D%7D%60&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "api error 19"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_formats_timestamp_with_date_helper() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ __timestamp__ | date \"2006-01-02\" }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "1970-01-01"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_converts_epoch_strings_with_unix_to_time_helper() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ \"1679577215000\" | unixToTime | date \"2006-01-02\" }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "2023-03-23"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_parses_dates_with_to_date_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ \"2021-11-02\" | toDate \"2006-01-02\" | unixEpoch }} {{ \"2021-11-02\" | toDateInZone \"2006-01-02\" \"America/New_York\" | unixEpoch }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/result/0/values") == Some(&json!([["19", "1635811200 1635825600"]]))
    );
}

#[tokio::test]
async fn query_range_endpoint_line_format_exposes_now_template_helper() {
    let state = fixture();
    let app = loki_router(state);
    let before = current_unix_epoch_nanos();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ now | unixEpochNanos }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let after = current_unix_epoch_nanos();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    let value = body
        .pointer("/data/result/0/values/0/1")
        .and_then(Value::as_str)
        .unwrap()
        .parse::<u128>()
        .unwrap();
    assert!(value >= before);
    assert!(value <= after);
}

#[tokio::test]
async fn query_range_endpoint_line_format_ranges_over_from_json_arrays() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ range $q := fromJson "[{\"query\":\"rate\",\"duration\":30},{\"query\":\"sum\",\"duration\":15}]" }}{{ $q.query }}={{ $q.duration }};{{ end }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "rate=30;sum=15;"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_ranges_with_current_dot_over_from_json_arrays() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ range fromJson "[{\"query\":\"rate\",\"duration\":30},{\"query\":\"sum\",\"duration\":15}]" }}{{ .query }}={{ .duration }};{{ end }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "rate=30;sum=15;"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_ranges_with_index_and_value_variables() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ range $i, $q := fromJson "[{\"query\":\"rate\",\"duration\":30},{\"query\":\"sum\",\"duration\":15}]" }}{{ $i }}:{{ $q.query }}={{ $q.duration }};{{ end }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "0:rate=30;1:sum=15;"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_ranges_over_from_json_objects() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ range $name, $duration := fromJson "{\"rate\":30,\"sum\":15}" }}{{ $name }}={{ $duration }};{{ end }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "rate=30;sum=15;"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_uses_range_else_for_empty_from_json_arrays() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ range $q := fromJson "[]" }}{{ $q.query }};{{ else }}none{{ end }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "none"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_go_template_index_and_slice_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ index (fromJson "{\"servers\":[{\"name\":\"api\"},{\"name\":\"worker\"}],\"status\":200}") "servers" 1 "name" }}|{{ slice "abcdef" 1 4 }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "worker|bcd"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_integer_math_template_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ add 3 2 5 }} {{ sub 5 2 }} {{ mul 5 2 3 }} {{ div 10 2 }} {{ mod 10 3 }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "10 3 30 5 1"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_float_math_template_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ addf 3.5 2 5 }} {{ subf 5.5 2 1.5 }} {{ mulf 5.5 2 2.5 }} {{ divf 10 2 4 }} {{ ceil 123.001 }} {{ round 123.555555 3 }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/result/0/values")
            == Some(&json!([["19", "10.5 2 27.5 1.25 124 123.556"]]))
    );
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_base64_template_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ \"hello\" | b64enc }} {{ \"aGVsbG8=\" | b64dec }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "aGVsbG8= hello"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_measurement_template_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ \"1m30s\" | duration }} {{ \"250ms\" | duration_seconds }} {{ \"1.5MiB\" | bytes }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "90 0.25 1572864"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_printf_template_helper() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ printf \"status=%25s\" \"500\" }} {{ printf \"%25-5.5s\" \"GET\" }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "status=500 GET  "]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_go_template_print_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ print \"status=\" 500 }}|{{ urlquery \"a=1 b=two\" }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/result/0/values") == Some(&json!([["19", "status=500|a%3D1+b%3Dtwo"]]))
    );
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_go_template_escape_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ html \"<a&b>\\\"'\" }}|{{ js \"line\\n\\\"quote\\\" <tag> &=\" }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/result/0/values")
            == Some(&json!([[
                "19",
                r#"&lt;a&amp;b&gt;&#34;&#39;|line\u000A\"quote\" \u003Ctag\u003E \u0026\u003D"#
            ]]))
    );
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_conditional_template_blocks() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ if contains \"error\" __line__ }}error{{ else }}other{{ end }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "error"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_control_template_variable_declarations() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ if $line := __line__ }}line={{ $line }}{{ else }}missing={{ $line }}{{ end }}|{{ with $payload := fromJson "{\"route\":\"checkout\"}" }}route={{ .route }}/{{ $payload.route }}{{ else }}missing={{ $payload }}{{ end }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/result/0/values")
            == Some(&json!([["19", "line=api error|route=checkout/checkout"]]))
    );
}

#[tokio::test]
async fn query_range_endpoint_line_format_can_reference_root_fields() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ with fromJson "{\"status\":\"200\"}" }}inner={{ .status }} root={{ $.app }}{{ end }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "inner=200 root=api"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_json_template_truthiness() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ if fromJson "[]" }}array{{ else }}empty-array{{ end }}|{{ if fromJson "{}" }}object{{ else }}empty-object{{ end }}|{{ if fromJson "0" }}number{{ else }}empty-number{{ end }}|{{ with fromJson "{\"method\":\"GET\"}" }}{{ .method }}{{ else }}missing{{ end }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/result/0/values")
            == Some(&json!([[
                "19",
                "empty-array|empty-object|empty-number|GET"
            ]]))
    );
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_else_with_template_blocks() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ with .missing }}primary={{ . }}{{ else with fromJson "{\"fallback\":\"worker\"}" }}fallback={{ .fallback }}{{ else }}none{{ end }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "fallback=worker"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_boolean_template_combinators() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ if and (contains \"error\" __line__) (not (contains \"debug\" __line__)) }}matched{{ else }}other{{ end }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "matched"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_ordering_template_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ if and (gt 2 1) (le 2 2) }}matched{{ else }}other{{ end }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "matched"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_template_variable_assignments() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ $line := __line__ }}seen={{ $line }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "seen=api error"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_reassigns_template_variables() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ $line := __line__ }}{{ $line = print "seen=" $line }}{{ $line }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "seen=api error"]])));
}
