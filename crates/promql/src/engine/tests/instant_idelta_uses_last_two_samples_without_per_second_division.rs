use super::*;

#[tokio::test]
pub(crate) async fn instant_idelta_uses_last_two_samples_without_per_second_division() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(0_i64, 0.0), (60_000, 1.0), (90_000, 3.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "temperature_celsius"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "idelta(temperature_celsius[2m])", 90_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(approx_eq(float_value(&samples[0].value), 2.0));
}
