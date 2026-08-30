use super::*;

#[tokio::test]
pub(crate) async fn rejects_tenant_id_with_unsupported_character() {
    // dskit ValidTenantID rejects characters outside [a-zA-Z0-9] and the
    // allowed punctuation set; '/' is forbidden.
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));

    let response = prometheus_router(state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=up")
                .header("x-scope-orgid", "tenant/a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert2::assert!(body["status"].as_str() == Some("error"));
    assert2::assert!(body["errorType"].as_str() == Some("bad_data"));
    assert2::assert!(
        body["error"].as_str() == // The reason comes from the shared `krabka_metrics::validate_tenant`.
        Some("tenant ID contains unsupported character `/`")
    );
}
