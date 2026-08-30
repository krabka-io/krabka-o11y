use super::*;

#[tokio::test]
pub(crate) async fn cardinality_active_series_rejects_over_tenant_limit() {
    let mut store = InMemoryMetricStore::new();
    let mut api_labels = Labels::new();
    api_labels.insert("__name__", "up");
    api_labels.insert("job", "api");
    store.push_float("tenant-a", api_labels, 0, 1.0);
    let mut worker_labels = Labels::new();
    worker_labels.insert("__name__", "up");
    worker_labels.insert("job", "worker");
    store.push_float("tenant-a", worker_labels, 0, 1.0);

    let limits = Limits {
        max_fetched_series_per_query: 1,
        ..Limits::default()
    };
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default())
            .with_query_limits(OverridesProvider::new(limits)),
    );

    let response = prometheus_router(state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/cardinality/active_series")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status() == StatusCode::UNPROCESSABLE_ENTITY);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert2::assert!(body["status"].as_str() == Some("error"));
    assert2::assert!(body["errorType"].as_str() == Some("execution"));
    assert2::assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains("series per query exceeded"))
    );
}
