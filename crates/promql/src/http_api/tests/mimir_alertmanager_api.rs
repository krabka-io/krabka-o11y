use serde_json::Value;

use super::*;

fn alertmanager_router_for_state(state: Arc<PrometheusApiState<InMemoryMetricStore>>) -> Router {
    authenticate_requests(
        super::super::mimir_alertmanager_router(state),
        &ServerSecurity::default(),
    )
}

#[tokio::test]
async fn ruler_router_reserves_root_alerts_for_alertmanager_config() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = authenticate_requests(
        super::super::mimir_ruler_prometheus_router(Arc::clone(&state))
            .merge(super::super::mimir_alertmanager_router(state)),
        &ServerSecurity::default(),
    );
    check!(
        request(&app, "GET", "/api/v1/alerts", Body::empty())
            .await
            .status()
            == StatusCode::NOT_FOUND
    );
    check!(
        request(&app, "GET", "/prometheus/api/v1/alerts", Body::empty())
            .await
            .status()
            == StatusCode::OK
    );
}

async fn request(
    app: &Router,
    method: &str,
    uri: &str,
    body: impl Into<Body>,
) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/json")
                .body(body.into())
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn multitenant_config_round_trips_and_deletes() {
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default())
            .with_mimir_config_store(Arc::clone(&object_store)),
    );
    let app = alertmanager_router_for_state(state);
    let missing = request(&app, "GET", "/api/v1/alerts", Body::empty()).await;
    check!(missing.status() == StatusCode::NOT_FOUND);
    let invalid = "template_files: {}\nalertmanager_config: |\n  route:\n    receiver: missing\n  receivers:\n    - name: default\n";
    check!(
        request(&app, "POST", "/api/v1/alerts", invalid)
            .await
            .status()
            == StatusCode::BAD_REQUEST
    );
    let config = "template_files: {}\nalertmanager_config: |\n  route:\n    receiver: default\n  receivers:\n    - name: default\n";
    check!(
        request(&app, "POST", "/api/v1/alerts", config)
            .await
            .status()
            == StatusCode::CREATED
    );
    let stored = request(&app, "GET", "/api/v1/alerts", Body::empty()).await;
    check!(stored.status() == StatusCode::OK);
    check!(to_bytes(stored.into_body(), usize::MAX).await.unwrap() == config);

    let restarted = Arc::new(
        PrometheusApiState::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default())
            .with_mimir_config_store(object_store),
    );
    restarted.reload_mimir_configs().await.unwrap();
    let restarted = alertmanager_router_for_state(restarted);
    let stored = request(&restarted, "GET", "/api/v1/alerts", Body::empty()).await;
    check!(to_bytes(stored.into_body(), usize::MAX).await.unwrap() == config);
    check!(
        request(&restarted, "DELETE", "/api/v1/alerts", Body::empty())
            .await
            .status()
            == StatusCode::OK
    );
}

#[tokio::test]
async fn grafana_v2_alerts_and_silences_are_tenant_scoped() {
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default())
            .with_mimir_config_store(Arc::clone(&object_store)),
    );
    let app = alertmanager_router_for_state(state);
    let alert = r#"[{"labels":{"alertname":"Down"},"annotations":{"summary":"down"}},{"labels":{"alertname":"Up"}}]"#;
    check!(
        request(&app, "POST", "/alertmanager/api/v2/alerts", alert)
            .await
            .status()
            == StatusCode::OK
    );
    let alerts = request(
        &app,
        "GET",
        "/alertmanager/api/v2/alerts?filter=alertname%3DDown",
        Body::empty(),
    )
    .await;
    check!(alerts.status() == StatusCode::OK);
    check!(alerts.headers()["cache-control"] == "no-store");
    let alerts: Value =
        serde_json::from_slice(&to_bytes(alerts.into_body(), usize::MAX).await.unwrap()).unwrap();
    check!(alerts[0]["labels"]["alertname"] == "Down");
    check!(
        alerts[0]["fingerprint"]
            .as_str()
            .is_some_and(|value| value.len() == 16)
    );
    check!(alerts[0]["status"]["state"] == "active");
    let groups = request(
        &app,
        "GET",
        "/alertmanager/api/v2/alerts/groups?filter=alertname%3DDown",
        Body::empty(),
    )
    .await;
    let groups: Value =
        serde_json::from_slice(&to_bytes(groups.into_body(), usize::MAX).await.unwrap()).unwrap();
    check!(groups[0]["alerts"].as_array().unwrap().len() == 1);

    let silence = r#"{"matchers":[{"name":"alertname","value":"Down","isRegex":false,"isEqual":true}],"startsAt":"2026-01-01T00:00:00Z","endsAt":"2026-01-02T00:00:00Z","createdBy":"grafana","comment":"maintenance"}"#;
    let created = request(&app, "POST", "/alertmanager/api/v2/silences", silence).await;
    let created: Value =
        serde_json::from_slice(&to_bytes(created.into_body(), usize::MAX).await.unwrap()).unwrap();
    let id = created["silenceID"].as_str().unwrap();
    let stored = request(
        &app,
        "GET",
        &format!("/alertmanager/api/v2/silence/{id}"),
        Body::empty(),
    )
    .await;
    check!(stored.status() == StatusCode::OK);

    let restarted = Arc::new(
        PrometheusApiState::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default())
            .with_mimir_config_store(object_store),
    );
    restarted.reload_mimir_configs().await.unwrap();
    let restarted = alertmanager_router_for_state(restarted);
    check!(
        request(
            &restarted,
            "GET",
            "/alertmanager/api/v2/alerts",
            Body::empty()
        )
        .await
        .status()
            == StatusCode::OK
    );
    check!(
        request(
            &restarted,
            "GET",
            &format!("/alertmanager/api/v2/silence/{id}"),
            Body::empty()
        )
        .await
        .status()
            == StatusCode::OK
    );
    check!(
        request(
            &restarted,
            "DELETE",
            &format!("/alertmanager/api/v2/silence/{id}"),
            Body::empty()
        )
        .await
        .status()
            == StatusCode::OK
    );
}
