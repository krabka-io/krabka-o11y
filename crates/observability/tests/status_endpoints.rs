//! The Loki status, ring, and ingester control endpoints.

mod support;

use assert2::{assert, check};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use krabka_observability::{
    InMemoryWalSink, QuerierIndexSource, Role, ServiceConfig, ServiceDependencies,
    build_service_router, distributor_router, loki_router,
};
use serde_json::{Value, json};
use support::{fixture, json_body, test_service_config, text_body};
use tower::ServiceExt as _;

#[tokio::test]
async fn status_ready_endpoint_returns_ok_for_loki_router() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(body.as_ref() == b"ready\n");
}

#[tokio::test]
async fn status_ready_endpoint_returns_ok_for_distributor_router() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(body.as_ref() == b"ready\n");
}

#[tokio::test]
async fn status_buildinfo_endpoint_returns_loki_build_info_json() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/status/buildinfo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body["version"] == env!("CARGO_PKG_VERSION"));
    for field in ["revision", "branch", "buildDate", "buildUser", "goVersion"] {
        assert!(body.get(field).and_then(Value::as_str).is_some());
    }
}

#[tokio::test]
async fn status_log_level_endpoint_returns_current_level() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/log_level")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == json!({"message": "Current log level is info"}));
}

#[tokio::test]
async fn status_log_level_endpoint_accepts_post_query_parameter() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/log_level?log_level=debug")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({"status": "success", "message": "Log level set to debug"})
    );
}

#[tokio::test]
async fn status_log_level_endpoint_accepts_form_post_body_for_distributor_router() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/log_level")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("log_level=warn"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({"status": "success", "message": "Log level set to warn"})
    );
}

#[tokio::test]
async fn status_log_level_endpoint_prefers_form_body_over_post_query_parameter() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/log_level?log_level=debug")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("log_level=warn"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({"status": "success", "message": "Log level set to warn"})
    );
}

#[tokio::test]
async fn status_log_level_endpoint_rejects_invalid_level() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/log_level?log_level=trace")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

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

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/log_level")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

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

    let response = app
        .oneshot(
            Request::builder()
                .uri("/config")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("target: all"));
}

#[tokio::test]
async fn status_config_diff_mode_returns_loki_error() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/config?mode=diff")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::INTERNAL_SERVER_ERROR);
    assert!(text_body(response).await == "unsupported type <nil>\n");
}

#[tokio::test]
async fn status_config_defaults_mode_returns_loki_defaults_lines() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/config?mode=defaults")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = text_body(response).await;
    assert!(body.contains("target: all\n"));
    assert!(body.contains("auth_enabled: true\n"));
}

#[tokio::test]
async fn distributor_router_exposes_loki_ingester_control_endpoints() {
    let app = distributor_router(InMemoryWalSink::default());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/flush")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ingester/prepare_shutdown")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::OK);
    assert!(text_body(response).await == "unset");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingester/prepare_shutdown")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ingester/prepare_shutdown")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(text_body(response).await == "set");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/ingester/prepare_shutdown")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ingester/prepare_shutdown")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(text_body(response).await == "unset");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ingester/shutdown?flush=false&delete_ring_tokens=false&terminate=false")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::NO_CONTENT);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingester/shutdown?flush=true&delete_ring_tokens=false&terminate=false")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn status_services_endpoint_returns_loki_service_states() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/services")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

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
        &test_service_config(Role::Compactor, tempfile::tempdir().unwrap().keep()),
        ServiceDependencies::default(),
        None,
    )
    .await
    .unwrap();

    for app in [querier, distributor, compactor] {
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/memberlist")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

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
        &test_service_config(Role::Compactor, tempfile::tempdir().unwrap().keep()),
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
        let response = app
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();

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
}

#[tokio::test]
async fn status_metrics_endpoint_returns_prometheus_text_for_loki_router() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    for needle in [
        "loki_build_info",
        "loki_boltdb_shipper_compactor_running",
        "krabka_observability_service_up",
        r#"component="querier""#,
    ] {
        check!(body.contains(needle));
    }
}

#[tokio::test]
async fn status_metrics_endpoint_returns_prometheus_text_for_distributor_router() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    for needle in [
        "loki_build_info",
        "loki_boltdb_shipper_compactor_running",
        "krabka_observability_service_up",
        r#"component="distributor""#,
    ] {
        check!(body.contains(needle));
    }
}

#[tokio::test]
async fn compactor_router_exposes_loki_status_and_ring_endpoints() {
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
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let ready_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(ready_response.status() == StatusCode::OK);
    assert!(text_body(ready_response).await == "ready\n");

    let services_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/services")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        text_body(services_response)
            .await
            .contains("compactor => Running")
    );

    let metrics_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(metrics_response.status() == StatusCode::OK);
    let metrics = text_body(metrics_response).await;
    assert!(metrics.contains("krabka_observability_service_up"));
    assert!(metrics.contains(r#"component="compactor""#));

    let config_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/config")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(config_response.status() == StatusCode::OK);
    assert!(text_body(config_response).await.contains("target: all"));

    let ring_response = app
        .oneshot(
            Request::builder()
                .uri("/compactor/ring")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

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

    let response = app
        .oneshot(
            Request::builder()
                .uri("/distributor/ring")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

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
