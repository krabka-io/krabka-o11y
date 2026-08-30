use super::*;

#[tokio::test]
pub(crate) async fn instant_delta_is_gauge_delta_without_reset_correction() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(30_000_i64, 4.0), (60_000, 3.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "temperature_celsius"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "delta(temperature_celsius[1m])", 60_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(approx_eq(float_value(&samples[0].value), -2.0));
}
