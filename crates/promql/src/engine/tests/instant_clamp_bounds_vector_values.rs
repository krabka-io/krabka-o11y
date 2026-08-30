use super::*;

#[tokio::test]
pub(crate) async fn instant_clamp_bounds_vector_values() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("low", -5.0), ("mid", 7.0), ("high", 20.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "temperature_celsius"), ("instance", instance)]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "clamp(temperature_celsius, 0, 10)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 3);
    let values = samples
        .iter()
        .map(|sample| {
            (
                sample.labels.get("instance").unwrap().to_string(),
                float_value(&sample.value),
            )
        })
        .collect::<Vec<_>>();
    check!(values.contains(&("low".to_string(), 0.0)));
    check!(values.contains(&("mid".to_string(), 7.0)));
    check!(values.contains(&("high".to_string(), 10.0)));
}
