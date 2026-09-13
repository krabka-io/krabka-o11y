use super::*;

async fn post_group(app: &Router, namespace: &str, name: &str, rule: &str) {
    let body = format!("name: {name}\nrules:\n  - record: {rule}\n    expr: vector(1)\n");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/prometheus/config/v1/rules/{namespace}"))
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/yaml")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    check!(response.status() == StatusCode::ACCEPTED);
}

async fn rules(app: &Router, query: &str) -> serde_json::Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/prometheus/api/v1/rules{query}"))
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    check!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn mimir_rule_filters_and_pagination_share_one_stable_order() {
    let app = prometheus_router(Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    )));
    post_group(&app, "a", "one", "first").await;
    post_group(&app, "a", "two", "second").await;
    post_group(&app, "b", "three", "third").await;

    let page = rules(&app, "?group_limit=1").await;
    check!(page["data"]["groups"][0]["name"] == "one");
    let token = page["data"]["groupNextToken"].as_str().unwrap();
    let page = rules(&app, &format!("?group_limit=1&group_next_token={token}")).await;
    check!(page["data"]["groups"][0]["name"] == "two");

    let filtered = rules(&app, "?file=a&rule_group=two&rule_name=second").await;
    check!(filtered["data"]["groups"].as_array().unwrap().len() == 1);
    check!(filtered["data"]["groups"][0]["rules"][0]["name"] == "second");

    let bracketed = rules(&app, "?file=ignored&file[]=b&rule_name[]=third").await;
    check!(bracketed["data"]["groups"].as_array().unwrap().len() == 1);
    check!(bracketed["data"]["groups"][0]["name"] == "three");
}

#[tokio::test]
async fn mimir_rule_output_includes_the_321_fields() {
    let app = prometheus_router(Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    )));
    post_group(&app, "team", "recording", "up:recorded").await;

    let body = rules(&app, "?type=RECORD").await;
    let group = &body["data"]["groups"][0];
    check!(group["sourceTenants"] == serde_json::json!([]));
    check!(group.get("lastError").is_none());
    check!(group.get("limit").is_none());
    check!(group["rules"][0]["labels"] == serde_json::json!({}));
}

#[tokio::test]
async fn ruler_config_reloads_from_object_store_after_restart() {
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default())
            .with_mimir_config_store(Arc::clone(&object_store)),
    );
    let app = prometheus_router(state);
    post_group(&app, "team", "recording", "up:recorded").await;

    let restarted = Arc::new(
        PrometheusApiState::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default())
            .with_mimir_config_store(object_store),
    );
    restarted.reload_mimir_configs().await.unwrap();
    let app = prometheus_router(restarted);
    let body = rules(&app, "").await;
    check!(body["data"]["groups"][0]["name"] == "recording");
}
