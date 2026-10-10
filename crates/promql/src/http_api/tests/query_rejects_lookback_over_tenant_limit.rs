use super::*;

#[tokio::test]
pub(crate) async fn query_rejects_lookback_over_tenant_limit() {
    let limits = Limits {
        max_query_lookback: minutes(1),
        ..Limits::default()
    };
    let response = limited_get(
        InMemoryMetricStore::new(),
        limits,
        "/api/v1/query?query=up&time=0",
    )
    .await;

    assert_execution_error(
        &response,
        |error| error.contains("query lookback exceeded"),
        "query",
    );
}
