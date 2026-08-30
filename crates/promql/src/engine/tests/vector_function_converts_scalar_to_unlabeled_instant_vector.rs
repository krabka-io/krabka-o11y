use super::*;

#[tokio::test]
pub(crate) async fn vector_function_converts_scalar_to_unlabeled_instant_vector() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "vector(2 * 3)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.is_empty());
    check!(samples[0].ts_ms == 10_000);
    check!(approx_eq(float_value(&samples[0].value), 6.0));
}
