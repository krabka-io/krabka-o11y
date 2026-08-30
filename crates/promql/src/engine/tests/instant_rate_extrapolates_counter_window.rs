use super::*;

#[tokio::test]
pub(crate) async fn instant_rate_extrapolates_counter_window() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [
        (0_i64, 0.0),
        (60_000, 1.0),
        (120_000, 2.0),
        (180_000, 3.0),
        (240_000, 4.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "http_requests_total"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "rate(http_requests_total[5m])", 300_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 5.0 / 300.0));
}
