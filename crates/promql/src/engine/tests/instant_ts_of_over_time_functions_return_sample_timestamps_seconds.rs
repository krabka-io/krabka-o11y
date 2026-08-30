use super::*;

#[tokio::test]
pub(crate) async fn instant_ts_of_over_time_functions_return_sample_timestamps_seconds() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [
        (0_i64, 10.0),
        (60_000, 3.0),
        (120_000, 7.0),
        (180_000, 3.0),
        (240_000, 11.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "queue_depth"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        ("ts_of_first_over_time(queue_depth[4m])", 60.0),
        ("ts_of_last_over_time(queue_depth[4m])", 240.0),
        ("ts_of_min_over_time(queue_depth[4m])", 180.0),
        ("ts_of_max_over_time(queue_depth[4m])", 240.0),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 240_000)
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
