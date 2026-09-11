use super::*;

#[tokio::test]
pub(crate) async fn promql_evaluation_rejects_series_over_tenant_limit() {
    let uris = [
        "/api/v1/query?query=count(up)&time=0",
        "/api/v1/query_range?query=count(up)&start=0&end=60&step=60",
    ];

    for uri in uris {
        let limits = Limits {
            max_fetched_series_per_query: 1,
            ..Limits::default()
        };
        let state = Arc::new(
            PrometheusApiState::new(Arc::new(two_series_store()), EngineOpts::default())
                .with_query_limits(OverridesProvider::new(limits)),
        );

        let response = prometheus_router(state)
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert2::assert!(
            response.status() == StatusCode::UNPROCESSABLE_ENTITY,
            "{uri}"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(body["status"].as_str() == Some("error"), "{uri}");
        assert2::assert!(body["errorType"].as_str() == Some("execution"), "{uri}");
        assert2::assert!(
            body["error"].as_str() == Some("series per query exceeded: observed 2 above limit 1"),
            "{uri}"
        );
    }
}
