use super::*;

#[tokio::test]
pub(crate) async fn discovery_rejects_label_counts_over_tenant_series_limit() {
    let uris = [
        "/api/v1/labels?start=0&end=1",
        "/api/v1/label/job/values?start=0&end=1",
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
