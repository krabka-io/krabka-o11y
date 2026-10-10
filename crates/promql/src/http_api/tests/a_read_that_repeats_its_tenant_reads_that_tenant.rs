use super::*;

/// The pinned Mimir 2.16.1 image reads `a|a` as the one tenant `a`, because it
/// counts distinct tenants. A read that repeats its tenant must see that
/// tenant's series, and not a rejection or another tenant's data.
#[tokio::test]
pub(crate) async fn a_read_that_repeats_its_tenant_reads_that_tenant() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(two_series_store()),
        EngineOpts::default(),
    ));

    let (status, body) = org_query_body(
        state,
        OrgRequest {
            uri: "/api/v1/series?match[]=up&start=0&end=1",
            org_id: "tenant-a|tenant-a",
        },
    )
    .await;

    assert2::assert!(status == StatusCode::OK);
    assert2::check!(body["data"].as_array().map(Vec::len) == Some(2));
}
