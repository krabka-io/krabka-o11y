use super::*;

#[tokio::test]
pub(crate) async fn range_rate_uses_each_step_as_window_end() {
    let engine = requests_counter_engine(5);
    let result = engine
        .query_range(
            &tenant_id("tenant-a"),
            "rate(http_requests_total[5m])",
            240_000,
            300_000,
            millis(60_000),
        )
        .await
        .unwrap();

    let series = lone_matrix_series(&result);
    check_series_points(
        series,
        &[
            ExpectedPoint {
                ts_ms: 240_000,
                value: 4.0 / 300.0,
            },
            ExpectedPoint {
                ts_ms: 300_000,
                value: 5.0 / 300.0,
            },
        ],
    );
}
