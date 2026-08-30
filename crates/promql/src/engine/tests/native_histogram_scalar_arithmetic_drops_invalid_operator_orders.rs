use super::*;

#[tokio::test]
pub(crate) async fn native_histogram_scalar_arithmetic_drops_invalid_operator_orders() {
    let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
    for query in [
        "histogram_count(2 / request_duration_seconds)",
        "histogram_count(request_duration_seconds + 2)",
    ] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert2::assert!(samples.is_empty());
    }
}
