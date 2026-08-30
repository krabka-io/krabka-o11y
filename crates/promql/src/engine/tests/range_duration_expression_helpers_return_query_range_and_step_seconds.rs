
#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn range_duration_expression_helpers_return_query_range_and_step_seconds() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());

    for (query, expected) in [
        ("range()", 120.0),
        ("step()", 30.0),
        ("start()", 60.0),
        ("end()", 180.0),
    ] {
        let result = engine
            .query_range("tenant-a", query, 60_000, 180_000, millis(30_000))
            .await
            .unwrap();

        let QueryResult::RangeMatrix(series) = result else {
            panic!("expected matrix");
        };
        assert2::assert!(series.len() == 1);
        assert2::assert!(series[0].labels.len() == 0);
        assert2::assert!(
            series[0]
                .samples
                .iter()
                .map(|(_, value)| float_value(value))
                .collect::<Vec<_>>()
                == vec![expected; 5]
        );
    }
}
