use super::*;

#[tokio::test]
pub(crate) async fn instant_timestamp_returns_sample_timestamp_seconds() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        60_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "timestamp(up)", 120_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(samples[0].ts_ms == 120_000);
    check!(approx_eq(float_value(&samples[0].value), 60.0));
}
