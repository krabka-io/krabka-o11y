use super::*;

mod scalar_function_converts_single_sample_vector_and_nan_otherwise {
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
}

mod scalar_max_of_min_of_require_experimental_feature {
    #[cfg(not(feature = "experimental-functions"))]
    use super::*;

    #[cfg(not(feature = "experimental-functions"))]
    #[tokio::test]
    pub(crate) async fn scalar_max_of_min_of_require_experimental_feature() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
        for query in ["max_of(1, 2)", "min_of(1, 2)"] {
            let error = engine
                .query_instant(&tenant_id("tenant-a"), query, 10_000)
                .await
                .unwrap_err();

            assert2::assert!(matches!(error, PromqlError::Unsupported(_)));
            assert2::assert!(format!("{error}").contains("experimental-functions"));
        }
    }
}

mod scalar_max_of_min_of_return_larger_and_smaller_scalar {
    #[cfg(feature = "experimental-functions")]
    use super::*;

    #[cfg(feature = "experimental-functions")]
    #[derive(bon::Builder)]
    struct ScalarExpectation {
        query: &'static str,
        value: f64,
    }

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    pub(crate) async fn scalar_max_of_min_of_return_larger_and_smaller_scalar() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
        for expected in [
            ScalarExpectation::builder()
                .query("max_of(1, 2)")
                .value(2.0)
                .build(),
            ScalarExpectation::builder()
                .query("min_of(1, 2)")
                .value(1.0)
                .build(),
        ] {
            let result = engine
                .query_instant(&tenant_id("tenant-a"), expected.query, 10_000)
                .await
                .unwrap();
            assert2::assert!(
                result
                    == QueryResult::Scalar {
                        ts_ms: 10_000,
                        value: expected.value,
                    }
            );
        }
        for query in [
            "max_of(NaN, 2)",
            "max_of(2, NaN)",
            "min_of(NaN, 2)",
            "min_of(2, NaN)",
        ] {
            let result = engine
                .query_instant(&tenant_id("tenant-a"), query, 10_000)
                .await
                .unwrap();
            let QueryResult::Scalar { ts_ms, value } = result else {
                panic!("expected scalar for {query}");
            };
            assert2::assert!(ts_ms == 10_000, "{query}");
            assert2::assert!(value.is_nan(), "{query}: {value}");
        }
    }
}

mod scalar_pi_function_returns_pi_constant {
    use super::*;

    #[tokio::test]
    pub(crate) async fn scalar_pi_function_returns_pi_constant() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
        let result = engine
            .query_instant(&tenant_id("tenant-a"), "pi()", 10_000)
            .await
            .unwrap();
        assert2::assert!(
            result
                == QueryResult::Scalar {
                    ts_ms: 10_000,
                    value: std::f64::consts::PI,
                }
        );
    }
}

mod unary_minus_negates_scalar_expression {
    use super::*;

    #[tokio::test]
    pub(crate) async fn unary_minus_negates_scalar_expression() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
        let result = engine
            .query_instant(&tenant_id("tenant-a"), "-(2 * 3)", 10_000)
            .await
            .unwrap();
        assert2::assert!(
            result
                == QueryResult::Scalar {
                    ts_ms: 10_000,
                    value: -6.0
                }
        );
    }
}

mod unary_minus_negates_vector_values_and_drops_metric_name {
    use super::*;

    #[tokio::test]
    pub(crate) async fn unary_minus_negates_vector_values_and_drops_metric_name() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "temperature_celsius"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]),
            10_000,
            3.5,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(&engine, "-temperature_celsius", 10_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            -3.5,
        );
    }
}
