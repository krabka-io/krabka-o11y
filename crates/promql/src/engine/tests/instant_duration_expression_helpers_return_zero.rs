#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn instant_duration_expression_helpers_return_zero() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());

    for query in ["range()", "step()", "start()", "end()"] {
        let result = engine
            .query_instant("tenant-a", query, 120_000)
            .await
            .unwrap();

        assert2::assert!(
            result
                == QueryResult::Scalar {
                    ts_ms: 120_000,
                    value: 0.0,
                }
        );
    }
}
