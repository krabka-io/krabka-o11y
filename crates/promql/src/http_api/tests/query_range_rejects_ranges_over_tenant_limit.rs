use super::*;

#[tokio::test]
pub(crate) async fn query_range_rejects_ranges_over_tenant_limit() {
    let limits = Limits {
        max_query_length: minutes(1),
        ..Limits::default()
    };
    let response = limited_get(
        InMemoryMetricStore::new(),
        limits,
        "/api/v1/query_range?query=up&start=0&end=120&step=60",
    )
    .await;

    assert_execution_error(
        &response,
        |error| error.contains("query range too long"),
        "query_range",
    );
}
