use super::*;

#[tokio::test]
pub(crate) async fn range_selector_offset_shifts_matrix_window_backwards() {
    let store = SeriesFixture::new(labels(&[("__name__", "up")]))
        .at(0_i64, 0.0)
        .at(60_000, 1.0)
        .at(120_000, 2.0)
        .at(180_000, 3.0)
        .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(&tenant_id("tenant-a"), "up[2m] offset 1m", 180_000)
        .await
        .unwrap();

    let series = lone_matrix_series(&result);
    check_series_points(
        series,
        &[
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
