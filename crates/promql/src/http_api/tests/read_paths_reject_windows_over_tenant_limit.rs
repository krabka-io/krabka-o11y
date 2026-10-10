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
        let response = limited_get(InMemoryMetricStore::new(), limits, uri).await;

        assert_execution_error(
            &response,
            |error| error.contains("query range too long"),
            uri,
        );
    }
}
