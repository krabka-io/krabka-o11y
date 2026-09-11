//! `X-Scope-OrgID` at every logs boundary, answered as Loki 3.5.1 answers it.
//!
//! Every expected status, content type and body below was captured from the
//! pinned Loki image (`mirror.gcr.io/grafana/loki@sha256:a7459453…`) running
//! the `loki_differential` configuration, which sets `auth_enabled: true`.
//! Loki does not answer a tenant error in one way: the auth middleware answers
//! a missing tenant, and each handler answers a malformed one through its own
//! error path. Several rows are a 500 for a client error. That is upstream's
//! answer, so do not change a row to a status that looks more correct.
//!
//! A plain-text answer written through Go's `http.Error` also carries
//! `X-Content-Type-Options: nosniff`. The ruler writes its JSON error body itself,
//! so its malformed-tenant rows carry no such header.

mod support;

use std::collections::BTreeMap;

use assert2::check;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use krabka_observability::{
    InMemoryWalSink, Role, ServiceDependencies, build_service_router, distributor_router,
    loki_router,
};
use serde_json::json;
use support::{fixture, test_service_config};
use tower::ServiceExt as _;

const TEXT: &str = "text/plain; charset=utf-8";
const SLASH: &str = "tenant ID 'a/b' contains unsupported character '/'";
const TOO_LONG: &str = "tenant ID is too long: max 150 characters";
const DOT_DOT: &str = "tenant ID is '.' or '..'";
const MULTIPLE: &str = "multiple org IDs present";

// The status, the content type, the body, and the `X-Content-Type-Options` header.
type Answer = (StatusCode, String, Vec<u8>, Option<String>);

/// The header values of the oracle table, and a name for each.
fn header_values_for_test() -> Vec<(&'static str, Option<String>)> {
    vec![
        ("absent", None),
        ("empty", Some(String::new())),
        ("a/b", Some("a/b".to_string())),
        ("151 characters", Some("x".repeat(151))),
        ("..", Some("..".to_string())),
        ("a|b", Some("a|b".to_string())),
    ]
}

fn answer_for_test(status: StatusCode, content_type: &str, body: impl Into<Vec<u8>>) -> Answer {
    (
        status,
        content_type.to_string(),
        body.into(),
        // Go's `http.Error` sends `nosniff` with every plain-text answer.
        (content_type == TEXT).then(|| "nosniff".to_string()),
    )
}

fn missing_for_test() -> Answer {
    answer_for_test(StatusCode::UNAUTHORIZED, TEXT, "no org id\n")
}

async fn call_for_test(
    app: &Router,
    method: &str,
    uri: &str,
    tenant: Option<&str>,
    body: (&str, String),
) -> Answer {
    let mut request = Request::builder().method(method).uri(uri);
    if let Some(tenant) = tenant {
        request = request.header("X-Scope-OrgID", tenant);
    }
    if !body.0.is_empty() {
        request = request.header("content-type", body.0);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body.1)).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let nosniff = response
        .headers()
        .get("x-content-type-options")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap()
        .to_vec();
    (status, content_type, body, nosniff)
}

/// Checks one surface against its oracle rows, in the order of
/// [`header_values_for_test`]. `None` skips a row.
async fn check_surface_for_test(
    app: &Router,
    method: &str,
    uri: &str,
    body: (&str, &str),
    expected: [Option<Answer>; 6],
) {
    for ((name, tenant), expected) in header_values_for_test().into_iter().zip(expected) {
        let Some(expected) = expected else {
            continue;
        };
        let actual = call_for_test(
            app,
            method,
            uri,
            tenant.as_deref(),
            (body.0, body.1.to_string()),
        )
        .await;
        check!(
            (
                actual.0,
                &actual.1,
                String::from_utf8_lossy(&actual.2),
                &actual.3,
            ) == (
                expected.0,
                &expected.1,
                String::from_utf8_lossy(&expected.2),
                &expected.3,
            ),
            "{method} {uri} with {name}"
        );
    }
}

fn push_body_for_test() -> String {
    json!({
        "streams": [{
            "stream": {"app": "api"},
            "values": [["1", "a line that must not reach the WAL"]]
        }]
    })
    .to_string()
}

#[tokio::test]
async fn a_loki_push_answers_every_oracle_row_and_writes_nothing() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let text = |body: &str| {
        Some(answer_for_test(
            StatusCode::BAD_REQUEST,
            TEXT,
            format!("{body}\n"),
        ))
    };

    for uri in ["/loki/api/v1/push", "/api/prom/push"] {
        check_surface_for_test(
            &app,
            "POST",
            uri,
            ("application/json", &push_body_for_test()),
            [
                Some(missing_for_test()),
                Some(missing_for_test()),
                text(SLASH),
                text(TOO_LONG),
                text(DOT_DOT),
                text(MULTIPLE),
            ],
        )
        .await;
    }

    check!(sink.records().is_empty());
}

#[tokio::test]
async fn a_loki_push_with_one_tenant_named_twice_goes_to_that_tenant() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let answer = call_for_test(
        &app,
        "POST",
        "/loki/api/v1/push",
        Some("tenant-a|tenant-a"),
        ("application/json", push_body_for_test()),
    )
    .await;

    check!(answer.0 == StatusCode::NO_CONTENT);
    check!(
        sink.records()
            .iter()
            .map(|record| record.tenant.as_str())
            .collect::<Vec<_>>()
            == ["tenant-a"]
    );
}

#[tokio::test]
async fn an_otlp_push_answers_every_oracle_row_and_writes_nothing() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    // A `google.rpc.Status` with only its `message` field set.
    let status = |message: &str| {
        let mut body = vec![0x12, u8::try_from(message.len()).unwrap()];
        body.extend_from_slice(message.as_bytes());
        Some(answer_for_test(
            StatusCode::BAD_REQUEST,
            "application/octet-stream",
            body,
        ))
    };
    let otlp_body = json!({
        "resourceLogs": [{
            "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "api"}}]},
            "scopeLogs": [{"logRecords": [{"timeUnixNano": "1", "body": {"stringValue": "line"}}]}]
        }]
    })
    .to_string();

    for uri in ["/otlp/v1/logs", "/v1/logs"] {
        check_surface_for_test(
            &app,
            "POST",
            uri,
            ("application/json", &otlp_body),
            [
                Some(missing_for_test()),
                Some(missing_for_test()),
                status(SLASH),
                status(TOO_LONG),
                status(DOT_DOT),
                status(MULTIPLE),
            ],
        )
        .await;
    }

    check!(sink.records().is_empty());
}

#[tokio::test]
async fn a_query_answers_every_oracle_row_by_its_query_kind() {
    let app = loki_router(fixture());
    let log_query = "query=%7Bapp%3D%22api%22%7D";
    let metric_query = "query=count_over_time(%7Bapp%3D%22api%22%7D%5B1m%5D)";
    let plain = |status, body: &str| Some(answer_for_test(status, TEXT, body));
    let rpc = |body: &str| {
        plain(
            StatusCode::BAD_REQUEST,
            &format!("rpc error: code = Code(400) desc = {body}"),
        )
    };

    let cases = [
        (
            format!("/loki/api/v1/query_range?{log_query}&start=0&end=100"),
            [rpc(SLASH), rpc(TOO_LONG), rpc(DOT_DOT)],
        ),
        (
            format!("/loki/api/v1/query_range?{metric_query}&start=0&end=100&step=10"),
            [
                plain(StatusCode::BAD_REQUEST, SLASH),
                plain(StatusCode::BAD_REQUEST, TOO_LONG),
                plain(StatusCode::BAD_REQUEST, DOT_DOT),
            ],
        ),
        (
            format!("/loki/api/v1/query?{log_query}&time=100"),
            [
                plain(StatusCode::INTERNAL_SERVER_ERROR, SLASH),
                plain(StatusCode::INTERNAL_SERVER_ERROR, TOO_LONG),
                plain(StatusCode::INTERNAL_SERVER_ERROR, DOT_DOT),
            ],
        ),
        (
            format!("/loki/api/v1/query?{metric_query}&time=100"),
            [
                plain(StatusCode::BAD_REQUEST, SLASH),
                plain(StatusCode::BAD_REQUEST, TOO_LONG),
                plain(StatusCode::BAD_REQUEST, DOT_DOT),
            ],
        ),
        (
            format!("/api/prom/query?{log_query}&time=100"),
            [rpc(SLASH), rpc(TOO_LONG), rpc(DOT_DOT)],
        ),
    ];

    for (uri, [slash, too_long, dot_dot]) in cases {
        // `a|b` is skipped here: Krabka federates it, as the next test shows.
        check_surface_for_test(
            &app,
            "GET",
            &uri,
            ("", ""),
            [
                Some(missing_for_test()),
                Some(missing_for_test()),
                slash,
                too_long,
                dot_dot,
                None,
            ],
        )
        .await;
    }
}

/// Loki federates `a|b` only with `querier.multi_tenant_queries_enabled` on,
/// and the `loki_differential` configuration leaves it off, where Loki answers
/// 500 `multiple org IDs present`. Krabka keeps the multi-tenant query, so it
/// answers as Loki does with the switch on.
#[tokio::test]
async fn a_query_that_names_two_tenants_runs_across_both() {
    let app = loki_router(fixture());

    for tenants in ["tenant-a|tenant-b", "tenant-a||tenant-b"] {
        let answer = call_for_test(
            &app,
            "GET",
            "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=0&end=100",
            Some(tenants),
            ("", String::new()),
        )
        .await;
        check!(answer.0 == StatusCode::OK, "{tenants}");
    }
}

#[tokio::test]
async fn the_label_series_and_index_reads_answer_every_oracle_row() {
    let app = loki_router(fixture());
    let plain = |status, body: &str| Some(answer_for_test(status, TEXT, body));
    let window = "start=0&end=100";
    let selector = "%7Bapp%3D%22api%22%7D";

    let bad_request_reads = [
        format!("/loki/api/v1/labels?{window}"),
        format!("/loki/api/v1/label/app/values?{window}"),
        format!("/loki/api/v1/series?match%5B%5D={selector}&{window}"),
        format!("/loki/api/v1/index/stats?query={selector}&{window}"),
        format!("/loki/api/v1/index/volume?query={selector}&{window}"),
        format!("/loki/api/v1/index/volume_range?query={selector}&{window}&step=10"),
        format!("/loki/api/v1/detected_labels?query={selector}&{window}"),
        format!("/api/prom/label?{window}"),
        format!("/api/prom/series?match%5B%5D={selector}&{window}"),
    ];
    for uri in bad_request_reads {
        check_surface_for_test(
            &app,
            "GET",
            &uri,
            ("", ""),
            [
                Some(missing_for_test()),
                Some(missing_for_test()),
                plain(StatusCode::BAD_REQUEST, SLASH),
                plain(StatusCode::BAD_REQUEST, TOO_LONG),
                plain(StatusCode::BAD_REQUEST, DOT_DOT),
                plain(StatusCode::INTERNAL_SERVER_ERROR, MULTIPLE),
            ],
        )
        .await;
    }

    check_surface_for_test(
        &app,
        "GET",
        &format!("/loki/api/v1/detected_fields?query={selector}&{window}"),
        ("", ""),
        [
            Some(missing_for_test()),
            Some(missing_for_test()),
            plain(
                StatusCode::BAD_REQUEST,
                &format!("rpc error: code = Code(400) desc = {SLASH}"),
            ),
            plain(
                StatusCode::BAD_REQUEST,
                &format!("rpc error: code = Code(400) desc = {TOO_LONG}"),
            ),
            plain(
                StatusCode::BAD_REQUEST,
                &format!("rpc error: code = Code(400) desc = {DOT_DOT}"),
            ),
            plain(StatusCode::INTERNAL_SERVER_ERROR, MULTIPLE),
        ],
    )
    .await;

    check_surface_for_test(
        &app,
        "GET",
        &format!("/loki/api/v1/patterns?query={selector}&{window}&step=10"),
        ("", ""),
        [
            Some(missing_for_test()),
            Some(missing_for_test()),
            plain(StatusCode::INTERNAL_SERVER_ERROR, SLASH),
            plain(StatusCode::INTERNAL_SERVER_ERROR, TOO_LONG),
            plain(StatusCode::INTERNAL_SERVER_ERROR, DOT_DOT),
            plain(StatusCode::NOT_FOUND, ""),
        ],
    )
    .await;
}

#[tokio::test]
async fn the_ruler_answers_every_oracle_row_and_never_serves_tenant_fake() {
    let app = loki_router(fixture());
    // The ruler writes this body itself, not through `http.Error`, so the
    // pinned Loki image sends no `X-Content-Type-Options` header with it.
    let ruler = |message: &str| {
        Some((
            StatusCode::INTERNAL_SERVER_ERROR,
            TEXT.to_string(),
            format!(
                r#"{{"status":"error","data":null,"errorType":"server_error","error":"{message}"}}"#
            )
            .into_bytes(),
            None,
        ))
    };
    let rule_group = "name: api-errors\nrules:\n  - record: r\n    expr: sum(count_over_time({app=\"api\"}[1m]))\n";

    let loki_routes = [
        ("GET", "/loki/api/v1/rules", ""),
        ("GET", "/loki/api/v1/rules/default", ""),
        ("POST", "/loki/api/v1/rules/default", rule_group),
        ("DELETE", "/loki/api/v1/rules/default", ""),
        ("GET", "/loki/api/v1/rules/default/api-errors", ""),
        ("DELETE", "/loki/api/v1/rules/default/api-errors", ""),
        ("GET", "/api/prom/rules", ""),
        ("POST", "/api/prom/rules/default", rule_group),
    ];
    for (method, uri, body) in loki_routes {
        check_surface_for_test(
            &app,
            method,
            uri,
            (
                if body.is_empty() {
                    ""
                } else {
                    "application/yaml"
                },
                body,
            ),
            [
                Some(missing_for_test()),
                Some(missing_for_test()),
                ruler("no org id"),
                ruler("no org id"),
                ruler("no org id"),
                ruler("no org id"),
            ],
        )
        .await;
    }

    for uri in ["/prometheus/api/v1/rules", "/prometheus/api/v1/alerts"] {
        check_surface_for_test(
            &app,
            "GET",
            uri,
            ("", ""),
            [
                Some(missing_for_test()),
                Some(missing_for_test()),
                ruler("no valid org id found"),
                ruler("no valid org id found"),
                ruler("no valid org id found"),
                ruler("no valid org id found"),
            ],
        )
        .await;
    }
}

/// Loki serves the delete API only with retention and a delete-request store
/// enabled. These rows come from the pinned image with
/// `compactor.retention_enabled: true` and
/// `compactor.delete_request_store: filesystem` added to the
/// `loki_differential` configuration.
#[tokio::test]
async fn the_delete_api_answers_every_oracle_row() {
    let app = build_service_router(
        &test_service_config(Role::BlockBuilder, tempfile::tempdir().unwrap().keep()),
        ServiceDependencies::default(),
        None,
    )
    .await
    .unwrap();
    let text = |body: &str| {
        Some(answer_for_test(
            StatusCode::BAD_REQUEST,
            TEXT,
            format!("{body}\n"),
        ))
    };
    let create = "/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D&start=1";

    for (method, uri) in [
        ("GET", "/loki/api/v1/delete"),
        ("POST", create),
        ("PUT", create),
        ("DELETE", "/loki/api/v1/delete?request_id=abc"),
    ] {
        check_surface_for_test(
            &app,
            method,
            uri,
            ("", ""),
            [
                Some(missing_for_test()),
                Some(missing_for_test()),
                text(SLASH),
                text(TOO_LONG),
                text(DOT_DOT),
                text(MULTIPLE),
            ],
        )
        .await;
    }

    let listed = call_for_test(
        &app,
        "GET",
        "/loki/api/v1/delete",
        Some("tenant-a"),
        ("", String::new()),
    )
    .await;
    check!((listed.0, listed.2) == (StatusCode::OK, b"[]".to_vec()));
}

#[tokio::test]
async fn a_tenantless_route_needs_no_tenant() {
    let app = loki_router(fixture());
    let answer = call_for_test(
        &app,
        "GET",
        "/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D",
        None,
        ("", String::new()),
    )
    .await;
    check!(answer.0 == StatusCode::OK);
    check!(
        serde_json::from_slice::<BTreeMap<String, String>>(&answer.2).ok()
            == Some(BTreeMap::from([
                ("status".to_string(), "success".to_string()),
                ("data".to_string(), "{app=\"api\"}".to_string()),
            ]))
    );
}
