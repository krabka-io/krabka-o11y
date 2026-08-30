use super::*;

#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn scalar_max_of_min_of_return_larger_and_smaller_scalar() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    for (query, expected) in [("max_of(1, 2)", 2.0), ("min_of(1, 2)", 1.0)] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();
        assert2::assert!(
            result
                == QueryResult::Scalar {
                    ts_ms: 10_000,
                    value: expected,
                }
        );
    }
}
