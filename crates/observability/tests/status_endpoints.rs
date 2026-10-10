//! The Loki status, ring, and ingester control endpoints.
//!
//! `/log_level` past its parameter parsing lives in `log_level.rs`: setting a
//! level moves a process-wide filter, so checking that it took needs a test
//! binary whose steps are in a known order.

mod support;

use assert2::{assert, check};
use axum::{body::to_bytes, http::StatusCode};
use krabka_observability::{
    InMemoryWalSink, Role, ServiceConfig, ServiceDependencies, build_service_router,
    distributor_router, loki_router,
};
use serde_json::{Value, json};
use support::{
    Method, fixture, json_body, minimal_service_config, send_bare, test_service_config, text_body,
};

#[tokio::test]
async fn status_ready_endpoint_returns_ok_for_loki_router() {
    let state = fixture();
    let app = loki_router(state);

    let response = send_bare(&app, Method::GET, "/ready").await;

    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(body.as_ref() == b"ready\n");
}

#[tokio::test]
async fn status_ready_endpoint_returns_ok_for_distributor_router() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink);

    let response = send_bare(&app, Method::GET, "/ready").await;

    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(body.as_ref() == b"ready\n");
}

#[tokio::test]
async fn status_buildinfo_endpoint_returns_loki_build_info_json() {
    let state = fixture();
    let app = loki_router(state);

    let response = send_bare(&app, Method::GET, "/loki/api/v1/status/buildinfo").await;

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body["version"] == env!("CARGO_PKG_VERSION"));
    for field in ["revision", "branch", "buildDate", "buildUser", "goVersion"] {
        assert!(body.get(field).and_then(Value::as_str).is_some());
    }
}

#[tokio::test]
async fn status_log_level_endpoint_rejects_invalid_level() {
    let state = fixture();
    let app = loki_router(state);

    let response = send_bare(&app, Method::POST, "/log_level?log_level=trace").await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        json_body(response).await
            == json!({"status": "failed", "message": "unrecognized log level \"trace\""})
    );
}

#[tokio::test]
async fn status_log_level_endpoint_rejects_missing_level_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = send_bare(&app, Method::POST, "/log_level").await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        json_body(response).await
            == json!({"status": "failed", "message": "unrecognized log level \"\""})
    );
}

#[tokio::test]
async fn status_config_endpoint_returns_loki_yaml_placeholder() {
    let state = fixture();
    let app = loki_router(state);

    let response = send_bare(&app, Method::GET, "/config").await;

    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("target: all"));
}

#[tokio::test]
async fn status_config_diff_mode_returns_loki_error() {
    let state = fixture();
    let app = loki_router(state);

    let response = send_bare(&app, Method::GET, "/config?mode=diff").await;

    assert!(response.status() == StatusCode::INTERNAL_SERVER_ERROR);
    assert!(text_body(response).await == "unsupported type <nil>\n");
}

#[tokio::test]
async fn status_config_defaults_mode_returns_loki_defaults_lines() {
    let state = fixture();
    let app = loki_router(state);

    let response = send_bare(&app, Method::GET, "/config?mode=defaults").await;

    assert!(response.status() == StatusCode::OK);
    let body = text_body(response).await;
    assert!(body.contains("target: all\n"));
    assert!(body.contains("auth_enabled: true\n"));
}

#[tokio::test]
async fn distributor_router_exposes_loki_ingester_control_endpoints() {
    let app = distributor_router(InMemoryWalSink::default());

    let response = send_bare(&app, Method::POST, "/flush").await;
    assert!(response.status() == StatusCode::NO_CONTENT);

    let response = send_bare(&app, Method::GET, "/ingester/prepare_shutdown").await;
    assert!(response.status() == StatusCode::OK);
    assert!(text_body(response).await == "unset");

    let response = send_bare(&app, Method::POST, "/ingester/prepare_shutdown").await;
    assert!(response.status() == StatusCode::NO_CONTENT);

    let response = send_bare(&app, Method::GET, "/ingester/prepare_shutdown").await;
    assert!(text_body(response).await == "set");

    let response = send_bare(&app, Method::DELETE, "/ingester/prepare_shutdown").await;
    assert!(response.status() == StatusCode::NO_CONTENT);

    let response = send_bare(&app, Method::GET, "/ingester/prepare_shutdown").await;
    assert!(text_body(response).await == "unset");

    let response = send_bare(
        &app,
        Method::GET,
        "/ingester/shutdown?flush=false&delete_ring_tokens=false&terminate=false",
    )
    .await;
    assert!(response.status() == StatusCode::NO_CONTENT);

    let response = send_bare(
        &app,
        Method::POST,
        "/ingester/shutdown?flush=true&delete_ring_tokens=false&terminate=false",
    )
    .await;
    assert!(response.status() == StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn status_services_endpoint_returns_loki_service_states() {
    let state = fixture();
    let app = loki_router(state);

    let response = send_bare(&app, Method::GET, "/services").await;

    assert!(response.status() == StatusCode::OK);
    let body = text_body(response).await;
    for needle in [
        "server => Running\n",
        "querier => Running\n",
        "distributor => Running\n",
        "compactor => Running\n",
    ] {
        check!(body.contains(needle));
    }
}

#[tokio::test]
async fn status_memberlist_endpoint_reports_memberlist_not_configured() {
    let state = fixture();
    let querier = loki_router(state);
    let distributor = distributor_router(InMemoryWalSink::default());
    let compactor = build_service_router(
        &test_service_config(Role::BlockBuilder, tempfile::tempdir().unwrap().keep()),
        ServiceDependencies::default(),
        None,
    )
    .await
    .unwrap();

    for app in [querier, distributor, compactor] {
        let response = send_bare(&app, Method::GET, "/memberlist").await;

        assert!(response.status() == StatusCode::OK);
        assert!(text_body(response).await == "This instance doesn't use memberlist.");
    }
}

#[tokio::test]
async fn status_ring_aliases_return_loki_ring_pages() {
    let state = fixture();
    let querier = loki_router(state);
    let distributor = distributor_router(InMemoryWalSink::default());
    let compactor = build_service_router(
        &test_service_config(Role::BlockBuilder, tempfile::tempdir().unwrap().keep()),
        ServiceDependencies::default(),
        None,
    )
    .await
    .unwrap();

    for (app, path) in [
        (querier.clone(), "/ring"),
        (querier, "/scheduler/ring"),
        (distributor, "/ring"),
        (compactor, "/ring"),
    ] {
        let response = send_bare(&app, Method::GET, path).await;

        check_active_ring_page(response).await;
    }
}

/// Reads `/metrics` from `app` and checks that it carries Loki's build and
/// compactor series and the service-up series of `component`.
async fn check_metrics_page_for_component(app: &axum::Router, component: &str) {
    let response = send_bare(app, Method::GET, "/metrics").await;

    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    let component_label = format!(r#"component="{component}""#);
    for needle in [
        "loki_build_info",
        "loki_boltdb_shipper_compactor_running",
        "krabka_observability_service_up",
        component_label.as_str(),
    ] {
        check!(body.contains(needle));
    }
}

#[tokio::test]
async fn status_metrics_endpoint_returns_prometheus_text_for_loki_router() {
    let state = fixture();
    let app = loki_router(state);

    check_metrics_page_for_component(&app, "querier").await;
}

#[tokio::test]
async fn status_metrics_endpoint_returns_prometheus_text_for_distributor_router() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink);

    check_metrics_page_for_component(&app, "distributor").await;
}

#[tokio::test]
async fn compactor_router_exposes_loki_status_and_ring_endpoints() {
    let config = ServiceConfig {
        index_prefix: Some("observability/logs".to_string()),
        ..minimal_service_config(Role::BlockBuilder)
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let ready_response = send_bare(&app, Method::GET, "/ready").await;
    assert!(ready_response.status() == StatusCode::OK);
    assert!(text_body(ready_response).await == "ready\n");

    let services_response = send_bare(&app, Method::GET, "/services").await;
    assert!(
        text_body(services_response)
            .await
            .contains("compactor => Running")
    );

    let metrics_response = send_bare(&app, Method::GET, "/metrics").await;
    assert!(metrics_response.status() == StatusCode::OK);
    let metrics = text_body(metrics_response).await;
    assert!(metrics.contains("krabka_observability_service_up"));
    // `compactor`, not `block-builder`: these are `Loki`'s ops strings, and a
    // `Loki` dashboard keyed on them is what they exist for. See
    // `BLOCK_BUILDER_OPS`.
    assert!(metrics.contains(r#"component="compactor""#));

    let config_response = send_bare(&app, Method::GET, "/config").await;
    assert!(config_response.status() == StatusCode::OK);
    assert!(text_body(config_response).await.contains("target: all"));

    let ring_response = send_bare(&app, Method::GET, "/compactor/ring").await;

    assert!(ring_response.status() == StatusCode::OK);
    let content_type = ring_response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let ring_body = text_body(ring_response).await;
    check!(content_type.starts_with("text/html"));
    check!(ring_body.contains("Ring Status"));
    check!(ring_body.contains("ACTIVE"));
}

#[tokio::test]
async fn distributor_ring_endpoint_returns_loki_status_page() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink);

    let response = send_bare(&app, Method::GET, "/distributor/ring").await;

    check_active_ring_page(response).await;
}

/// Checks the `/ready`, `/services` and `/ring` pages of a distributor that
/// is serving.
async fn check_serving_pages(app: &axum::Router) {
    let (status, ready) = page(app, Method::GET, "/ready").await;
    check!(status == StatusCode::OK);
    check!(ready == "ready\n");
    let (_, services) = page(app, Method::GET, "/services").await;
    check!(services.contains("distributor => Running\n"));
    let (_, ring) = page(app, Method::GET, "/ring").await;
    check!(ring.contains("ACTIVE"));
}

async fn check_active_ring_page(response: axum::response::Response) {
    assert!(response.status() == StatusCode::OK);
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = text_body(response).await;
    check!(content_type.starts_with("text/html"));
    check!(body.contains("Ring Status"));
    check!(body.contains("ACTIVE"));
}

async fn page(app: &axum::Router, method: Method, uri: &str) -> (StatusCode, String) {
    let response = send_bare(app, method, uri).await;
    let status = response.status();
    (status, text_body(response).await)
}

/// `/services` used to answer `Running` for every module from the moment the
/// listener bound, whatever `/ready` said. Two pages describing one process
/// have to describe the same process, so this drives the one transition an
/// operator can cause over HTTP -- a drain -- and reads all three pages at
/// each step.
#[tokio::test]
async fn services_ring_and_ready_agree_across_a_drain() {
    let app = distributor_router(InMemoryWalSink::default());

    check_serving_pages(&app).await;

    let (status, _) = page(&app, Method::POST, "/ingester/prepare_shutdown").await;
    check!(status == StatusCode::NO_CONTENT);

    let (status, ready) = page(&app, Method::GET, "/ready").await;
    check!(status == StatusCode::SERVICE_UNAVAILABLE);
    check!(ready == "not ready: accepting-writes\n");
    let (_, services) = page(&app, Method::GET, "/services").await;
    check!(services.contains("distributor => Stopping\n"));
    // The listener answered this request, so it alone keeps running.
    check!(services.contains("server => Running\n"));
    check!(!services.contains("distributor => Running\n"));
    let (_, ring) = page(&app, Method::GET, "/ring").await;
    check!(ring.contains("JOINING"));
    check!(!ring.contains("ACTIVE"));

    let (status, _) = page(&app, Method::DELETE, "/ingester/prepare_shutdown").await;
    check!(status == StatusCode::NO_CONTENT);

    check_serving_pages(&app).await;
}
