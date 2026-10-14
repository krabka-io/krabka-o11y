use super::*;

#[tokio::test]
pub(crate) async fn instant_query_uses_hot_sample_newer_than_compacted_sample() {
    let labels = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    let store = merged_store_with_samples(&[up_sample(10_000, 1.0)], &[up_sample(20_000, 2.0)]);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(&tenant_id("tenant-a"), "up", 20_000)
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
                drop_name: false,
            }]
    );
}
