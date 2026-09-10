//! The format-query endpoint, over the `LogQL` surface it formats.

mod support;

use assert2::assert;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use krabka_observability::{
    InMemoryWalSink, QuerierIndexSource, Role, ServiceConfig, ServiceDependencies,
    build_service_router, distributor_router, loki_router,
};
use serde_json::json;
use support::{fixture, json_body};
use tower::ServiceExt as _;

#[tokio::test]
async fn format_query_endpoint_is_available_on_distributor_and_compactor_routers() {
    let distributor_app = distributor_router(InMemoryWalSink::default());

    let distributor_response = distributor_app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bfoo%3D%20%22bar%22%7D")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(distributor_response.status() == StatusCode::OK);
    assert!(
        json_body(distributor_response).await
            == json!({
                "status": "success",
                "data": "{foo=\"bar\"}"
            })
    );

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
    let compactor_app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let compactor_response = compactor_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/format_query")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("query=%7Bfoo%3D%20%22bar%22%7D"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(compactor_response.status() == StatusCode::OK);
    assert!(
        json_body(compactor_response).await
            == json!({
                "status": "success",
                "data": "{foo=\"bar\"}"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_returns_formatted_logql_query() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bfoo%3D%20%22bar%22%7D")
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
                "data": "{foo=\"bar\"}"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_accepts_form_encoded_post_body() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/format_query")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("query=%7Bfoo%3D%20%22bar%22%7D"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": "{foo=\"bar\"}"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_prefers_form_body_over_post_query_parameter() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("query=%7Bapp%3D%22worker%22%7D"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": "{app=\"worker\"}"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_accepts_form_post_query_with_raw_ampersand() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/format_query")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(r#"query={app="api"} |= "a&b""#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} |= "a&b""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_regex_field_filters() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20method%3D~%22GET%7CPOST%22%20%7C%20path!~%22%2Fhealth.*%22")
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
                "data": r#"{app="api"} | logfmt | method=~"GET|POST" | path!~"/health.*""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_backtick_field_filter_strings() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20msg%20%3D%20%60api%20error%60%20%7C%20path%20%3D~%20%60%2Fapi%2F.%2B%60")
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
                "data": r#"{app="api"} | logfmt | msg="api error" | path=~"/api/.+""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_pattern_parser_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20pattern%20%60%3Cmethod%3E%20%3Cpath%3E%20%28%3Cstatus%3E%29%20%3Cduration%3E%60%20%7C%20status%20%3E%3D%20500")
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
                "data": r#"{app="api"} | pattern "<method> <path> (<status>) <duration>" | status>=500"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_regexp_parser_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20regexp%20%60%28%3FP%3Cmethod%3E%5Cw%2B%29%20%28%3FP%3Cpath%3E%5B%5Cw%2F%5D%2B%29%20%5C%28%28%3FP%3Cstatus%3E%5Cd%2B%29%5C%29%20%28%3FP%3Cduration%3E.*%29%60%20%7C%20status%20%3E%3D%20500")
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
                "data": r#"{app="api"} | regexp "(?P<method>\\w+) (?P<path>[\\w/]+) \\((?P<status>\\d+)\\) (?P<duration>.*)" | status>=500"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_unpack_parser_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20unpack%20%7C%20pod%20%3D%20%22pod-3223f%22")
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
                "data": r#"{app="api"} | unpack | pod="pod-3223f""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_selected_json_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20json%20first_server%3D%22servers%5B0%5D%22%2C%20ua%3D%22request.headers%5B%5C%22User-Agent%5C%22%5D%22%20%7C%20ua%20%3D%20%22Agent%2F1%22")
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
                "data": r#"{app="api"} | json first_server="servers[0]", ua="request.headers[\"User-Agent\"]" | ua="Agent/1""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_parameterized_logfmt_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20host%2C%20fwd_ip%3D%22fwd%22%20%7C%20fwd_ip%20%3D%20%22124.133.124.161%22")
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
                "data": r#"{app="api"} | logfmt host, fwd_ip="fwd" | fwd_ip="124.133.124.161""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_line_format_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20line_format%20%60%7B%7B.msg%7D%7D%20%7B%7B.status%7D%7D%60")
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
                "data": r#"{app="api"} | logfmt | line_format "{{.msg}} {{.status}}""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_line_format_template_pipelines() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20line_format%20%60%7B%7B%20.path%20%7C%20replace%20%22%2F%22%20%22_%22%20%7C%20upper%20%7D%7D%60")
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
                "data": r#"{app="api"} | logfmt | line_format "{{ .path | replace \"/\" \"_\" | upper }}""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_additional_template_string_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20line_format%20%60%7B%7B%20.raw%20%7C%20trim%20%7C%20trimPrefix%20%22%2F%22%20%7C%20title%20%7D%7D%20%7B%7B%20.query%20%7C%20urlencode%20%7D%7D%60")
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
                "data": r#"{app="api"} | logfmt | line_format "{{ .raw | trim | trimPrefix \"/\" | title }} {{ .query | urlencode }}""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_logical_template_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20line_format%20%60%7B%7B%20contains%20%22timeout%22%20.msg%20%7D%7D%20%7B%7B%20.path%20%7C%20hasPrefix%20%22%2Fapi%22%20%7D%7D%20%7B%7B%20.path%20%7C%20hasSuffix%20%22items%22%20%7D%7D%20%7B%7B%20.method%20%7C%20eq%20%22GET%22%20%7D%7D%60")
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
                "data": r#"{app="api"} | logfmt | line_format "{{ contains \"timeout\" .msg }} {{ .path | hasPrefix \"/api\" }} {{ .path | hasSuffix \"items\" }} {{ .method | eq \"GET\" }}""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_spacing_template_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20line_format%20%60%7B%7B%20alignLeft%205%20.short%20%7D%7D%7C%7B%7B%20alignRight%205%20.long%20%7D%7D%7C%7B%7B%20repeat%203%20.mark%20%7D%7D%7C%7B%7B%20.multi%20%7C%20indent%202%20%7D%7D%60")
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
                "data": r#"{app="api"} | logfmt | line_format "{{ alignLeft 5 .short }}|{{ alignRight 5 .long }}|{{ repeat 3 .mark }}|{{ .multi | indent 2 }}""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_regex_template_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20line_format%20%60%7B%7B%20count%20%22o%22%20.word%20%7D%7D%7C%7B%7B%20regexReplaceAll%20%22%28f%29%28o%2B%29%22%20.word%20%22%24%7B1%7Da%22%20%7D%7D%7C%7B%7B%20regexReplaceAllLiteral%20%22%28f%29%28o%2B%29%22%20.word%20%22%24%7B1%7Da%22%20%7D%7D%60")
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
                "data": r#"{app="api"} | logfmt | line_format "{{ count \"o\" .word }}|{{ regexReplaceAll \"(f)(o+)\" .word \"${1}a\" }}|{{ regexReplaceAllLiteral \"(f)(o+)\" .word \"${1}a\" }}""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_label_format_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20label_format%20route%3Dpath%2C%20summary%3D%60%7B%7B.method%7D%7D%20%7B%7B.status%7D%7D%60")
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
                "data": r#"{app="api"} | logfmt | label_format route=path, summary="{{.method}} {{.status}}""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_accepts_label_replace_metric_query() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29")
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
                "data": r#"label_replace(count_over_time({app="api"} |= "error"[30s]),"service","$1-api","app","(.*)")"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_rejects_label_join_metric_query_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=label_join%28count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%2C%20%22joined%22%2C%20%22%2F%22%2C%20%22app%22%2C%20%22env%22%2C%20%22missing%22%29")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        json_body(response).await
            == json!({
                "status": "invalid-query",
                "error": "parse error at line 1, col 1: syntax error: unexpected IDENTIFIER"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_accepts_vector_function_expression() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=vector%282.5e-1%29")
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
                "data": "vector(0.250000)"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_label_replace_vector_function_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=label_replace%28vector%281%29%2C%20%22service%22%2C%20%22api-%241%22%2C%20%22missing%22%2C%20%22%28.%2A%29%22%29")
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
                "data": r#"label_replace(vector(1.000000),"service","api-$1","missing","(.*)")"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_label_replace_vector_expression_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=label_replace%28vector%281%29%2Bvector%282%29%2C%20%22service%22%2C%20%22api-%241%22%2C%20%22missing%22%2C%20%22%28.%2A%29%22%29")
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
                "data": r#"label_replace((vector(1.000000) + vector(2.000000)),"service","api-$1","missing","(.*)")"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_label_replace_metric_vector_expression_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%2B%20vector%281%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29")
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
                "data": "label_replace(\n  (count_over_time({app=\"api\"}[30s]) + vector(1.000000)),\n  \"service\",\n  \"$1-api\",\n  \"app\",\n  \"(.*)\"\n)"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_label_replace_metric_scalar_expression_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%2B%201.25e-1%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29")
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
                "data": r#"label_replace((count_over_time({app="api"}[30s]) + 0.125),"service","$1-api","app","(.*)")"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_rejects_label_join_vector_function_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=label_join%28vector%281%29%2C%20%22joined%22%2C%20%22%2F%22%2C%20%22app%22%2C%20%22missing%22%29")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        json_body(response).await
            == json!({
                "status": "invalid-query",
                "error": "parse error at line 1, col 1: syntax error: unexpected IDENTIFIER"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_vector_set_expression_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    for (query, expected) in [
        (
            "vector%281%29%20or%20vector%282%29",
            "(vector(1.000000) or vector(2.000000))",
        ),
        (
            "label_replace%28vector%281%29%2C%20%22service%22%2C%20%22api-%241%22%2C%20%22missing%22%2C%20%22%28.%2A%29%22%29%20or%20vector%282%29",
            r#"(label_replace(vector(1.000000),"service","api-$1","missing","(.*)") or vector(2.000000))"#,
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/loki/api/v1/format_query?query={query}"))
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
                    "data": expected
                })
        );
    }
}

#[tokio::test]
async fn format_query_endpoint_formats_vector_arithmetic_expression_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    for (query, expected) in [
        (
            "vector%281%29%2Bvector%282%29",
            "(vector(1.000000) + vector(2.000000))",
        ),
        (
            "label_replace%28vector%281%29%2C%20%22service%22%2C%20%22api-%241%22%2C%20%22missing%22%2C%20%22%28.%2A%29%22%29%2Bvector%282%29",
            r#"(label_replace(vector(1.000000),"service","api-$1","missing","(.*)") + vector(2.000000))"#,
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/loki/api/v1/format_query?query={query}"))
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
                    "data": expected
                })
        );
    }
}

#[tokio::test]
async fn format_query_endpoint_formats_metric_vector_arithmetic_expression_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    for (query, expected) in [
        (
            "count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%2Bvector%281%29",
            r#"(count_over_time({app="api"}[30s]) + vector(1.000000))"#,
        ),
        (
            "label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29%2Bvector%282%29",
            "  label_replace(count_over_time({app=\"api\"}[30s]),\"service\",\"$1-api\",\"app\",\"(.*)\")\n+\n  vector(2.000000)",
        ),
        (
            "label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%2B%20vector%281%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29%2Bvector%282%29",
            "  label_replace(\n    (count_over_time({app=\"api\"}[30s]) + vector(1.000000)),\n    \"service\",\n    \"$1-api\",\n    \"app\",\n    \"(.*)\"\n  )\n+\n  vector(2.000000)",
        ),
        (
            "label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%2B%201.25e-1%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29%2Bvector%282%29",
            "  label_replace((count_over_time({app=\"api\"}[30s]) + 0.125),\"service\",\"$1-api\",\"app\",\"(.*)\")\n+\n  vector(2.000000)",
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/loki/api/v1/format_query?query={query}"))
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
                    "data": expected
                })
        );
    }
}

#[tokio::test]
async fn format_query_endpoint_formats_metric_scalar_arithmetic_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%2B1.25e-1")
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
                "data": r#"(count_over_time({app="api"}[30s]) + 0.125)"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_scalar_metric_comparison_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=1%3Ebool%20count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29")
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
                "data": r#"(1 > bool count_over_time({app="api"}[30s]))"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_quantile_metric_vector_arithmetic_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=quantile_over_time%280.75%2C%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20unwrap%20cost%20%5B30s%5D%29%2Bvector%281%29")
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
                "data": r#"(quantile_over_time(0.75,{app="api"} | logfmt | unwrap cost[30s]) + vector(1.000000))"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_metric_vector_set_modifier_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    for (query, expected) in [
        (
            "count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20or%20on%28app%29%20vector%281%29",
            r#"(count_over_time({app="api"}[30s]) or on (app)  vector(1.000000))"#,
        ),
        (
            "label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29%20or%20vector%282%29",
            "  label_replace(count_over_time({app=\"api\"}[30s]),\"service\",\"$1-api\",\"app\",\"(.*)\")\nor\n  vector(2.000000)",
        ),
        (
            "label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%2B%20vector%281%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29%20or%20vector%282%29",
            "  label_replace(\n    (count_over_time({app=\"api\"}[30s]) + vector(1.000000)),\n    \"service\",\n    \"$1-api\",\n    \"app\",\n    \"(.*)\"\n  )\nor\n  vector(2.000000)",
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/loki/api/v1/format_query?query={query}"))
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
                    "data": expected
                })
        );
    }
}

#[tokio::test]
async fn format_query_endpoint_formats_vector_metric_arithmetic_expression_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=vector%281%29%2Bcount_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29")
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
                "data": r#"(vector(1.000000) + count_over_time({app="api"}[30s]))"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_metric_vector_matching_modifier_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%2Bon%28app%29vector%281%29")
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
                "data": r#"(count_over_time({app="api"}[30s]) + on (app)  vector(1.000000))"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_vector_metric_group_modifier_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=vector%281%29%2Bon%28app%29group_left%20count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29")
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
                "data": r#"(vector(1.000000) + on (app) group_left count_over_time({app="api"}[30s]))"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_vector_matching_modifier_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=vector%281%29%2Bon%28app%2Cenv%29vector%282%29")
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
                "data": "(vector(1.000000) + on (app,env)  vector(2.000000))"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_vector_group_modifier_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=vector%281%29%2Bon%28app%29group_left%28env%29vector%282%29")
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
                "data": "(vector(1.000000) + on (app) group_left (env) vector(2.000000))"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_vector_group_modifier_without_labels_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=vector%281%29%2Bon%28app%29group_left%20vector%282%29")
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
                "data": "(vector(1.000000) + on (app) group_left vector(2.000000))"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_metric_vector_bool_comparison_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    for (query, expected) in [
        (
            "count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%3Ebool%20vector%281%29",
            r#"(count_over_time({app="api"}[30s]) > bool vector(1.000000))"#,
        ),
        (
            "label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29%20%3E%20bool%20vector%282%29",
            "  label_replace(count_over_time({app=\"api\"}[30s]),\"service\",\"$1-api\",\"app\",\"(.*)\")\n> bool\n  vector(2.000000)",
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/loki/api/v1/format_query?query={query}"))
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
                    "data": expected
                })
        );
    }
}

#[tokio::test]
async fn format_query_endpoint_formats_vector_metric_comparison_group_modifier_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=vector%281%29%3Eon%28app%29group_left%20count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29")
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
                "data": r#"(vector(1.000000) > on (app) group_left count_over_time({app="api"}[30s]))"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_vector_comparison_expression_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=vector%281%29%3Ebool%20vector%282%29")
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
                "data": "(vector(1.000000) > bool vector(2.000000))"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_scalar_expression_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%281%2B2%29%2A3")
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
                "data": "9"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_metric_binary_arithmetic_query_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%2F%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29")
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
                "data": r#"(count_over_time({app="api"}[30s]) / count_over_time({app="api"} |= "error"[30s]))"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_vector_aggregation_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=sum%28rate%28%7Bapp%3D%22api%22%7D%7C%3D%22error%22%5B5m%5D%29%29%20by%20%28env%2Cstatus%29")
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
                "data": r#"sum by (env,status)(rate({app="api"} |= "error"[5m]))"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_sort_vector_expression_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    for (query, expected) in [
        (
            "sort%28count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%2B%20vector%281%29%29",
            r#"sort((count_over_time({app="api"}[30s]) + vector(1.000000)))"#,
        ),
        (
            "sort_desc%28vector%281%29%2Bcount_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%29",
            r#"sort_desc((vector(1.000000) + count_over_time({app="api"}[30s])))"#,
        ),
        (
            "sort%28label_replace%28vector%281%29%2C%20%22service%22%2C%20%22api-%241%22%2C%20%22missing%22%2C%20%22%28.%2A%29%22%29%29",
            r#"sort(label_replace(vector(1.000000),"service","api-$1","missing","(.*)"))"#,
        ),
        (
            "sort_desc%28label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%2B%20vector%281%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29%29",
            "sort_desc(\n  label_replace(\n    (count_over_time({app=\"api\"}[30s]) + vector(1.000000)),\n    \"service\",\n    \"$1-api\",\n    \"app\",\n    \"(.*)\"\n  )\n)",
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/loki/api/v1/format_query?query={query}"))
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
                    "data": expected
                })
        );
    }
}

#[tokio::test]
async fn format_query_endpoint_formats_metric_offsets_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    for (query, expected) in [
        (
            "count_over_time%28%7Bapp%3D%22api%22%7D%5B10s%5D%20offset%205m%29",
            r#"count_over_time({app="api"}[10s] offset 5m0s)"#,
        ),
        (
            "count_over_time%28%7Bapp%3D%22api%22%7D%5B10s%5D%20offset%201h%29",
            r#"count_over_time({app="api"}[10s] offset 1h0m0s)"#,
        ),
        (
            "count_over_time%28%7Bapp%3D%22api%22%7D%5B10s%5D%20offset%201500ms%29",
            r#"count_over_time({app="api"}[10s] offset 1.5s)"#,
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/loki/api/v1/format_query?query={query}"))
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
                    "data": expected
                })
        );
    }
}

#[tokio::test]
async fn format_query_endpoint_formats_range_grouping_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=quantile_over_time%28.75%2C%7Bapp%3D%22api%22%7D%7Clogfmt%7Cunwrap%20cost%5B30s%5D%29%20by%28app%29")
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
                "data": r#"quantile_over_time(0.75,{app="api"} | logfmt | unwrap cost[30s]) by (app)"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_accepts_range_selector_before_pipeline() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%20%7C%3D%20%22error%22%20%7C%20logfmt%20%7C%20status%20%3E%3D%20500%29")
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
                "data": r#"count_over_time({app="api"} |= "error" | logfmt | status>=500[30s])"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_accepts_approx_topk_metric_query() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=approx_topk%282%2C%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%29")
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
                "data": r#"approx_topk(2,count_over_time({app="api"} |= "error"[30s]))"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_metric_binary_comparison_query_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%3E%20bool%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29")
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
                "data": r#"(count_over_time({app="api"}[30s]) > bool count_over_time({app="api"} |= "error"[30s]))"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_metric_binary_set_query_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20and%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29")
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
                "data": r#"(count_over_time({app="api"}[30s]) and count_over_time({app="api"} |= "error"[30s]))"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_metric_binary_matching_modifier_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%2F%20ignoring%28app%29%20count_over_time%28%7Bapp%3D%22worker%22%7D%5B30s%5D%29")
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
                "data": r#"(count_over_time({app="api"}[30s]) / ignoring (app)  count_over_time({app="worker"}[30s]))"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_metric_binary_group_modifier_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=sum%20by%28app%2C%20env%29%28count_over_time%28%7Benv%3D%22prod%22%7D%5B30s%5D%29%29%20%2F%20on%28env%29%20group_left%20sum%20by%28env%29%28count_over_time%28%7Benv%3D%22prod%22%7D%5B30s%5D%29%29")
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
                "data": "  sum by (app,env)(count_over_time({env=\"prod\"}[30s]))\n/ on (env) group_left\n  sum by (env)(count_over_time({env=\"prod\"}[30s]))"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_decolorize_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20decolorize%20%7C%3D%20%22error%22")
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
                "data": r#"{app="api"} | decolorize |= "error""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_drop_and_keep_label_expression_stages() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20drop%20level%2C%20app%3D~%22debug-.*%22%20%7C%20keep%20method%2C%20status%3D%22500%22")
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
                "data": r#"{app="api"} | logfmt | drop level, app=~"debug-.*" | keep method, status="500""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_unwrap_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20unwrap%20cost%20%7C%20__error__%20%3D%20%22%22")
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
                "data": r#"{app="api"} | logfmt | unwrap cost | __error__="""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_unwrap_bytes_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20unwrap%20bytes%28size%29%20%7C%20__error__%20%3D%20%22%22")
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
                "data": r#"{app="api"} | logfmt | unwrap bytes(size) | __error__="""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_unwrap_duration_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20unwrap%20duration%28latency%29%20%7C%20__error__%20%3D%20%22%22")
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
                "data": r#"{app="api"} | logfmt | unwrap duration(latency) | __error__="""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_pattern_line_filters() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%3E%20%60%3C_%3E%20caller%3Dhttp.go%3A194%20level%3Ddebug%20%3C_%3E%60%20!%3E%20%60%3C_%3E%20healthcheck%20%3C_%3E%60")
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
                "data": r#"{app="api"} |> "<_> caller=http.go:194 level=debug <_>" !> "<_> healthcheck <_>""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_ignores_logql_comments() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%23%20selector%20comment%0A%7C%3D%20%22error%20%23%20literal%22%20%23%20line%20filter%20comment%0A%7C%20logfmt%20%23%20parser%20comment%0A%7C%20status%20%3E%3D%20500%20%23%20field%20filter%20comment")
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
                "data": r#"{app="api"} |= "error # literal" | logfmt | status>=500"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_duration_and_bytes_field_filters() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20duration%20%3E%3D%2020ms%20%7C%20bytes_consumed%20%3E%201.5MiB")
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
                "data": r#"{app="api"} | logfmt | duration>=20000000ns | bytes_consumed>1572864B"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_or_field_filter_chains() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20status%20%3E%3D%20500%20or%20level%20%3D%20%22warn%22")
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
                "data": r#"{app="api"} | logfmt | status>=500 or level="warn""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_parenthesized_field_filter_chains() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20duration%20%3E%3D%2020ms%20or%20%28method%20%3D%20%22GET%22%20and%20size%20%3C%3D%2020KB%29")
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
                "data": r#"{app="api"} | logfmt | duration>=20000000ns or (method="GET" and size<=20000B)"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_comma_and_adjacent_field_filter_chains() {
    let state = fixture();
    let app = loki_router(state);

    let comma_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20status%20%3E%3D%20500%2C%20path%20!~%20%22%2Fhealth.*%22")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(comma_response.status() == StatusCode::OK);
    assert!(
        json_body(comma_response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt | status>=500 and path!~"/health.*""#
            })
    );

    let adjacent_response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20status%20%3E%3D%20500%20path%20!~%20%22%2Fhealth.*%22")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(adjacent_response.status() == StatusCode::OK);
    assert!(
        json_body(adjacent_response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt | status>=500 and path!~"/health.*""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_returns_loki_error_for_invalid_logql() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bfoo%3D")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        json_body(response).await
            == json!({
                "status": "invalid-query",
                "error": "parse error at line 1, col 6: syntax error: unexpected $end, expecting STRING"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_rejects_signed_vector_function_literals_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=vector%28-2.5e-1%29")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        json_body(response).await
            == json!({
                "status": "invalid-query",
                "error": "parse error at line 1, col 8: syntax error: unexpected -, expecting NUMBER"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_rejects_unspaced_vector_set_operators_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    for query in [
        "vector%281%29orvector%282%29",
        "vector%281%29andvector%282%29",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/loki/api/v1/format_query?query={query}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(
            json_body(response).await
                == json!({
                    "status": "invalid-query",
                    "error": "parse error at line 1, col 10: syntax error: unexpected IDENTIFIER"
                })
        );
    }
}

#[tokio::test]
async fn format_query_endpoint_returns_loki_error_for_missing_query() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        json_body(response).await
            == json!({
                "status": "invalid-query",
                "error": "parse error : syntax error: unexpected $end"
            })
    );
}
