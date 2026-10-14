use super::*;

struct ClampedSample {
    instance: String,
    value: f64,
}

#[tokio::test]
pub(crate) async fn instant_clamp_bounds_vector_values() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("low", -5.0), ("mid", 7.0), ("high", 20.0)] {
        push_metric_instance_sample(&mut store, "temperature_celsius", instance, value);
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "clamp(temperature_celsius, 0, 10)", 10_000).await;
    assert2::assert!(samples.len() == 3);
    let values = samples
        .iter()
        .map(|sample| ClampedSample {
            instance: sample.labels.get("instance").unwrap().to_string(),
            value: float_value(&sample.value),
        })
        .collect::<Vec<_>>();
    assert!(
        values
            .iter()
            .any(|sample| sample.instance == "low" && sample.value == 0.0)
    );
    assert!(
        values
            .iter()
            .any(|sample| sample.instance == "mid" && sample.value == 7.0)
    );
    assert!(
        values
            .iter()
            .any(|sample| sample.instance == "high" && sample.value == 10.0)
    );
}
