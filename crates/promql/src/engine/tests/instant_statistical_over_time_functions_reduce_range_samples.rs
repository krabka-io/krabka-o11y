use super::*;

#[tokio::test]
pub(crate) async fn instant_statistical_over_time_functions_reduce_range_samples() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [
        (0_i64, 2.0),
        (60_000, 4.0),
        (120_000, 4.0),
        (180_000, 4.0),
        (240_000, 5.0),
        (300_000, 5.0),
        (360_000, 7.0),
        (420_000, 9.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "latency_seconds"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        ("stdvar_over_time(latency_seconds[8m])", 4.0),
        ("stddev_over_time(latency_seconds[8m])", 2.0),
        ("quantile_over_time(0.5, latency_seconds[8m])", 4.5),
        ("mad_over_time(latency_seconds[8m])", 0.5),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 420_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        check!(samples.len() == 1);
        check!(samples[0].labels.get("__name__").is_none());
        check!(samples[0].labels.get("job") == Some("api"));
        check!(approx_eq(float_value(&samples[0].value), expected));
    }
}
