use super::*;

#[tokio::test]
pub(crate) async fn query_range_rejects_resolution_over_point_cap_without_limits() {
    // No per-tenant query_limits configured: Prometheus enforces the
    // 11000-point resolution cap unconditionally. start=0 end=20000 step=1s
    // => 20000 intervals > 11000.
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));

    let response = prometheus_router(state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=up&start=0&end=20000&step=1")
                .header("x-scope-orgid", "tenant-a")
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
        body["error"].as_str()
            == Some(
                "exceeded maximum resolution of 11,000 points per timeseries. \
                 Try decreasing the query resolution (?step=XX)"
            )
    );
}
