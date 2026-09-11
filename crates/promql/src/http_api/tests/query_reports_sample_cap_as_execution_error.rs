use super::*;

#[tokio::test]
pub(crate) async fn query_reports_sample_cap_as_execution_error() {
    let limits = Limits {
        max_samples_per_query: 1,
        ..Limits::default()
    };
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(two_series_store()), EngineOpts::default())
            .with_query_limits(OverridesProvider::new(limits)),
    );

    let response = prometheus_router(state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=up&time=0")
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
        body["error"].as_str() == Some("samples per query exceeded: observed 2 above limit 1")
    );
}
