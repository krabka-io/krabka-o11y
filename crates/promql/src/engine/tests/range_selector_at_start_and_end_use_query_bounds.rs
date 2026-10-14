use super::*;

#[derive(bon::Builder)]
struct RangeSelectorExpectation {
    query: &'static str,
    value: f64,
}

#[tokio::test]
pub(crate) async fn range_selector_at_start_and_end_use_query_bounds() {
    let store = SeriesFixture::new(labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]))
    .at(60_000_i64, 1.0)
    .at(120_000, 2.0)
    .at(180_000, 3.0)
    .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for expected in [
        RangeSelectorExpectation::builder()
            .query("up @ start()")
            .value(1.0)
            .build(),
        RangeSelectorExpectation::builder()
            .query("up @ end()")
            .value(3.0)
            .build(),
    ] {
        let series = query_range_matrix(&engine, expected.query, 60_000).await;
        assert2::assert!(series.len() == 1);
        assert2::assert!(series[0].samples.len() == 3);
        for sample in &series[0].samples {
            let ts_ms = sample.timestamp_ms;
            let value = &sample.value;
            assert2::assert!([60_000, 120_000, 180_000].contains(&ts_ms));
            assert2::assert!(approx_eq(float_value(value), expected.value));
        }
    }
}
