use super::*;

#[tokio::test]
pub(crate) async fn instant_query_uses_hot_sample_newer_than_compacted_sample() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    let labels = labels(&[("__name__", "up"), ("job", "api")]);
    cold.push_float("tenant-a", labels.clone(), 10_000, 1.0);
    hot.push_float("tenant-a", labels.clone(), 20_000, 2.0);

    let store = MergedMetricStore::new(cold, hot);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "up", 20_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected instant vector");
    };
    assert2::assert!(
        samples
            == vec![InstantSample {
                labels,
                ts_ms: 20_000,
                value: SampleValue::Float(2.0),
            }]
    );
}
