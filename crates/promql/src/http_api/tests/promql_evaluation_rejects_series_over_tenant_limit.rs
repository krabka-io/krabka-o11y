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
        let response = limited_get(two_series_store(), limits, uri).await;

        assert_execution_error(
            &response,
            |error| error == "series per query exceeded: observed 2 above limit 1",
            uri,
        );
    }
}
