use super::*;

#[tokio::test]
pub(crate) async fn scalar_function_converts_single_sample_vector_and_nan_otherwise() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "single_value"), ("instance", "a")]),
        10_000,
        7.0,
    );
    for (instance, value) in [("a", 1.0), ("b", 2.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "multi_value"), ("instance", instance)]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let single = engine
        .query_instant("tenant-a", "scalar(single_value)", 10_000)
        .await
        .unwrap();
    assert2::assert!(
        single
            == QueryResult::Scalar {
                ts_ms: 10_000,
                value: 7.0,
            }
    );

    for query in ["scalar(missing_metric)", "scalar(multi_value)"] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();
        let QueryResult::Scalar { ts_ms, value } = result else {
            panic!("expected scalar");
        };
        assert2::assert!(ts_ms == 10_000);
        assert2::assert!(value.is_nan());
    }
}
