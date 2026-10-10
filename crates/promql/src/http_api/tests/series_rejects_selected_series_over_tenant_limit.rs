use super::*;

#[tokio::test]
pub(crate) async fn series_rejects_selected_series_over_tenant_limit() {
    let limits = Limits {
        max_fetched_series_per_query: 1,
        ..Limits::default()
    };
    let response = limited_get(
        two_series_store(),
        limits,
        "/api/v1/series?match[]=up&start=0&end=1",
    )
    .await;

    assert_execution_error(
        &response,
        |error| error.contains("series per query exceeded"),
        "series",
    );
}
