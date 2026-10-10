use super::*;

#[tokio::test]
pub(crate) async fn cardinality_active_series_rejects_over_tenant_limit() {
    let limits = Limits {
        max_fetched_series_per_query: 1,
        ..Limits::default()
    };
    let response = limited_get(
        two_series_store(),
        limits,
        "/api/v1/cardinality/active_series",
    )
    .await;

    assert_execution_error(
        &response,
        |error| error.contains("series per query exceeded"),
        "active series",
    );
}
