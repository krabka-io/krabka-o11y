use super::*;

/// The pinned Mimir 2.16.1 image reads `a|a` as the one tenant `a`, because it
/// counts distinct tenants. A read that repeats its tenant must see that
/// tenant's series, and not a rejection or another tenant's data.
#[tokio::test]
pub(crate) async fn a_read_that_repeats_its_tenant_reads_that_tenant() {
    let router = prometheus_router(Arc::new(PrometheusApiState::new(
        Arc::new(two_series_store()),
        EngineOpts::default(),
    )));

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v1/series?match[]=up&start=0&end=1")
                .header("x-scope-orgid", "tenant-a|tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert2::check!(body["data"].as_array().map(Vec::len) == Some(2));
}
