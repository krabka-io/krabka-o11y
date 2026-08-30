use super::*;

#[tokio::test]
pub(crate) async fn range_selector_at_start_and_end_use_query_bounds() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(60_000_i64, 1.0), (120_000, 2.0), (180_000, 3.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [("up @ start()", 1.0), ("up @ end()", 3.0)] {
        let result = engine
            .query_range("tenant-a", query, 60_000, 180_000, millis(60_000))
            .await
            .unwrap();

        let QueryResult::RangeMatrix(series) = result else {
            panic!("expected matrix");
        };
        assert2::assert!(series.len() == 1);
        assert2::assert!(series[0].samples.len() == 3);
        for (ts_ms, value) in &series[0].samples {
            assert2::assert!([60_000, 120_000, 180_000].contains(ts_ms));
            assert2::assert!(approx_eq(float_value(value), expected));
        }
    }
}
