use super::*;

#[tokio::test]
pub(crate) async fn promql_evaluation_rejects_series_over_tenant_limit() {
    let uris = [
        "/api/v1/query?query=count(up)&time=0",
        "/api/v1/query_range?query=count(up)&start=0&end=60&step=60",
    ];

    for uri in uris {
        assert_series_limit_rejected(uri).await;
    }
}
