use super::*;

#[tokio::test]
pub(crate) async fn instant_increase_corrects_counter_resets() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(0_i64, 1.0), (60_000, 2.0), (120_000, 1.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "http_requests_total"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "increase(http_requests_total[2m])", 120_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(approx_eq(float_value(&samples[0].value), 2.0));
}
