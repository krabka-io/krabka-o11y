use super::*;

#[tokio::test]
pub(crate) async fn range_selector_returns_samples_in_each_step_window() {
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels(&[("__name__", "up")]), 0, 0.0);
    store.push_float("tenant-a", labels(&[("__name__", "up")]), 60_000, 1.0);
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up")]),
        90_000,
        stale_nan(),
    );
    store.push_float("tenant-a", labels(&[("__name__", "up")]), 120_000, 2.0);
    store.push_float("tenant-a", labels(&[("__name__", "up")]), 180_000, 3.0);

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_range("tenant-a", "up[2m]", 120_000, 180_000, millis(60_000))
        .await
        .unwrap();

    let QueryResult::RangeMatrix(series) = result else {
        panic!("expected matrix");
    };
    check!(series.len() == 1);
    check!(series[0].samples.len() == 3);
    check!(series[0].samples[0].0 == 60_000);
    check!(series[0].samples[2].0 == 180_000);
    check!(series[0].samples.iter().all(|(_, value)| {
        let SampleValue::Float(value) = value else {
            return false;
        };
        value.to_bits() != stale_nan().to_bits()
    }));
}
