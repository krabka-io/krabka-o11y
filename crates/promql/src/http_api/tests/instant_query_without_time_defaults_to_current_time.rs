use super::*;

#[tokio::test]
pub(crate) async fn instant_query_without_time_defaults_to_current_time() {
    let mut store = InMemoryMetricStore::new();
    let mut labels = Labels::new();
    labels.insert("__name__", "up");
    labels.insert("job", "api");
    store.push_float("tenant-a", labels, unix_now_ms().unwrap(), 1.0);

    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));

    let response = prometheus_router(state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=up")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(body["data"]["result"][0]["metric"]["job"].as_str() == Some("api"));
    assert2::assert!(body["data"]["result"][0]["value"][1].as_str() == Some("1"));
}
