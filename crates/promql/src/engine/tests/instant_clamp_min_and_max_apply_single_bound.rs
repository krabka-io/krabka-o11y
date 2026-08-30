use super::*;

#[tokio::test]
pub(crate) async fn instant_clamp_min_and_max_apply_single_bound() {
    let mut store = InMemoryMetricStore::new();
    for (metric, value) in [("below", -5.0), ("inside", 7.0), ("above", 20.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "temperature_celsius"), ("case", metric)]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let min_result = engine
        .query_instant("tenant-a", "clamp_min(temperature_celsius, 0)", 10_000)
        .await
        .unwrap();
    let max_result = engine
        .query_instant("tenant-a", "clamp_max(temperature_celsius, 10)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(min_samples) = min_result else {
        panic!("expected vector");
    };
    let QueryResult::InstantVector(max_samples) = max_result else {
        panic!("expected vector");
    };
    check!(min_samples.len() == 3);
    check!(max_samples.len() == 3);
    check!(min_samples.iter().any(|sample| {
        sample.labels.get("case") == Some("below") && approx_eq(float_value(&sample.value), 0.0)
    }));
    check!(min_samples.iter().any(|sample| {
        sample.labels.get("case") == Some("above") && approx_eq(float_value(&sample.value), 20.0)
    }));
    check!(max_samples.iter().any(|sample| {
        sample.labels.get("case") == Some("below") && approx_eq(float_value(&sample.value), -5.0)
    }));
    check!(max_samples.iter().any(|sample| {
        sample.labels.get("case") == Some("above") && approx_eq(float_value(&sample.value), 10.0)
    }));
}
