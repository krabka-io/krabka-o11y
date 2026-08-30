use super::*;

#[tokio::test]
pub(crate) async fn instant_clamp_with_reversed_bounds_returns_empty_vector() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "temperature_celsius"), ("instance", "api")]),
        10_000,
        7.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "clamp(temperature_celsius, 10, 0)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.is_empty());
}
