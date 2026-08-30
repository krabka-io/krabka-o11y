use super::*;

#[tokio::test]
pub(crate) async fn instant_predict_linear_extrapolates_gauge_series() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(0_i64, 1.0), (60_000, 3.0), (120_000, 5.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "disk_free_bytes"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "predict_linear(disk_free_bytes[2m], 60)",
            120_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 7.0));
    // The same series over a window that admits all three samples. `[2m]`
    // starts exactly at the first one, and the window excludes its own start,
    // so only two ever reached the regression -- leaving a guard that refused
    // three samples with nothing to refuse.
    let QueryResult::InstantVector(samples) = engine
        .query_instant(
            "tenant-a",
            "predict_linear(disk_free_bytes[3m], 60)",
            120_000,
        )
        .await
        .expect("a prediction")
    else {
        panic!("expected a vector");
    };
    check!(samples.len() == 1, "three samples still predict");
    check!(approx_eq(float_value(&samples[0].value), 7.0));
}
