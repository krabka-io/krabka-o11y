use super::*;

#[tokio::test]
pub(crate) async fn range_query_accepts_parenthesized_expression() {
    let store = SeriesFixture::new(labels(&[("__name__", "up")]))
        .at(0_i64, 0.0)
        .at(60_000, 1.0)
        .at(120_000, 2.0)
        .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_range(&tenant_id("tenant-a"), "(up)", 0, 120_000, millis(60_000))
        .await
        .unwrap();

    let series = lone_matrix_series(&result);
    check_series_points(
        series,
        &[
            ExpectedPoint {
                ts_ms: 0,
                value: 0.0,
            },
            ExpectedPoint {
                ts_ms: 60_000,
                value: 1.0,
            },
            ExpectedPoint {
                ts_ms: 120_000,
                value: 2.0,
            },
        ],
    );
}
