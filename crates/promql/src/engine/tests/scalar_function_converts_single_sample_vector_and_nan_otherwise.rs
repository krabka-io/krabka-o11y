use super::*;

#[derive(bon::Builder)]
struct ScalarInputSample {
    instance: &'static str,
    value: f64,
}

#[tokio::test]
pub(crate) async fn scalar_function_converts_single_sample_vector_and_nan_otherwise() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "single_value"),
            crate::test_support::TestLabelPair::new("instance", "a"),
        ]),
        10_000,
        7.0,
    );
    for sample in [
        ScalarInputSample::builder()
            .instance("a")
            .value(1.0)
            .build(),
        ScalarInputSample::builder()
            .instance("b")
            .value(2.0)
            .build(),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "multi_value"),
                crate::test_support::TestLabelPair::new("instance", sample.instance),
            ]),
            10_000,
            sample.value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let single = engine
        .query_instant(&tenant_id("tenant-a"), "scalar(single_value)", 10_000)
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
            .query_instant(&tenant_id("tenant-a"), query, 10_000)
            .await
            .unwrap();
        let QueryResult::Scalar { ts_ms, value } = result else {
            panic!("expected scalar");
        };
        assert2::assert!(ts_ms == 10_000);
        assert2::assert!(value.is_nan());
    }
}
