use super::*;

#[tokio::test]
pub(crate) async fn read_paths_reject_windows_over_tenant_limit() {
    let uris = [
        "/api/v1/labels?start=0&end=120",
        "/api/v1/label/job/values?start=0&end=120",
        "/api/v1/series?match[]=up&start=0&end=120",
        "/api/v1/query_exemplars?query=up&start=0&end=120",
    ];

    for uri in uris {
        let limits = Limits {
            max_query_length: minutes(1),
            ..Limits::default()
        };
        let state = Arc::new(
            PrometheusApiState::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default())
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
            body["error"]
                .as_str()
                .is_some_and(|error| error.contains("query range too long")),
            "{uri}"
        );
    }
}
