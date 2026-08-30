use super::*;

#[tokio::test]
pub(crate) async fn range_query_accepts_parenthesized_expression() {
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels(&[("__name__", "up")]), 0, 0.0);
    store.push_float("tenant-a", labels(&[("__name__", "up")]), 60_000, 1.0);
    store.push_float("tenant-a", labels(&[("__name__", "up")]), 120_000, 2.0);

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_range("tenant-a", "(up)", 0, 120_000, millis(60_000))
        .await
        .unwrap();

    let QueryResult::RangeMatrix(series) = result else {
        panic!("expected matrix");
    };
    check!(series.len() == 1);
    check!(series[0].samples.len() == 3);
    for (sample, (want_ts, want)) in
        series[0]
            .samples
            .iter()
            .zip([(0, 0.0), (60_000, 1.0), (120_000, 2.0)])
    {
        check!(sample.0 == want_ts);
        check!(approx_eq(float_value(&sample.1), want), "at ts {want_ts}");
    }
}
