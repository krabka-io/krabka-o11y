use super::*;

#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn instant_double_exponential_smoothing_smooths_gauge_series() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [
        (0_i64, 3.0),
        (60_000, 6.0),
        (120_000, 12.0),
        (180_000, 21.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "queue_depth"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "double_exponential_smoothing(queue_depth[4m], 0.5, 0.5)",
            180_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 17.625));
}
