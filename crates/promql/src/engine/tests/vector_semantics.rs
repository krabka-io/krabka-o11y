use super::*;

mod vector_and_default_matching_ignores_metadata_labels {
    use super::*;

    #[tokio::test]
    pub(crate) async fn vector_and_default_set_matching_ignores_metadata_labels() {
        let mut store = InMemoryMetricStore::new();
        push_metadata_sample(&mut store, "a", 10.0);

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples =
            instant_vector(&engine, "(requests_total + 1) and requests_total", 10_000).await;
        check!(samples.len() == 1);
        check!(samples[0].value == SampleValue::Float(11.0));
    }
}

mod vector_and_keeps_left_samples_with_matching_right_key {
    use super::*;

    #[tokio::test]
    pub(crate) async fn vector_and_keeps_left_samples_with_matching_right_key() {
        let engine = PromqlEngine::new(Arc::new(set_op_store()), EngineOpts::default());
        let samples = instant_vector(&engine, "up and on (instance) target_info", 10_000).await;
        check!(samples.len() == 1);
        check!(samples[0].labels.get("__name__") == Some("up"));
        check!(samples[0].labels.get("instance") == Some("b"));
        check!(samples[0].labels.get("job") == Some("api"));
        check!(approx_eq(float_value(&samples[0].value), 2.0));
    }
}

mod vector_function_converts_scalar_to_unlabeled_instant_vector {
    use super::*;

    #[tokio::test]
    pub(crate) async fn vector_function_converts_scalar_to_unlabeled_instant_vector() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
        let samples = instant_vector(&engine, "vector(2 * 3)", 10_000).await;
        check!(samples.len() == 1);
        check!(samples[0].labels.is_empty());
        check!(samples[0].ts_ms == 10_000);
        check!(approx_eq(float_value(&samples[0].value), 6.0));
    }
}

mod vector_or_returns_left_union_unmatched_right_samples {
    use super::*;

    #[tokio::test]
    pub(crate) async fn vector_or_returns_left_union_unmatched_right_samples() {
        let engine = PromqlEngine::new(Arc::new(set_op_store()), EngineOpts::default());
        let samples = instant_vector(&engine, "up or on (instance) target_info", 10_000).await;
        check!(samples.len() == 3);
        check!(samples.iter().any(|sample| {
            sample.labels.get("__name__") == Some("up")
                && sample.labels.get("instance") == Some("a")
        }));
        check!(samples.iter().any(|sample| {
            sample.labels.get("__name__") == Some("up")
                && sample.labels.get("instance") == Some("b")
        }));
        check!(samples.iter().any(|sample| {
            sample.labels.get("__name__") == Some("target_info")
                && sample.labels.get("instance") == Some("c")
                && sample.labels.get("region") == Some("east")
                && approx_eq(float_value(&sample.value), 30.0)
        }));
    }
}

mod vector_scalar_arithmetic_preserves_labels_and_drops_metric_name {
    use super::*;

    #[tokio::test]
    pub(crate) async fn vector_scalar_arithmetic_preserves_labels_and_drops_metric_name() {
        let engine = FloatStore::default().up_job_sample("api", 1.0).engine();
        let samples = instant_vector(&engine, "up * 2", 10_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            2.0,
        );

        let samples = instant_vector(&engine, "2 - up", 10_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            1.0,
        );
    }
}

mod vector_scalar_atan2_preserves_labels_and_drops_metric_name {
    use super::*;

    #[tokio::test]
    pub(crate) async fn vector_scalar_atan2_preserves_labels_and_drops_metric_name() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "y"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]),
            10_000,
            1.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(&engine, "y atan2 0", 10_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            std::f64::consts::FRAC_PI_2,
        );
    }
}

mod vector_unless_keeps_left_samples_without_matching_right_key {
    use super::*;

    #[tokio::test]
    pub(crate) async fn vector_unless_keeps_left_samples_without_matching_right_key() {
        let engine = PromqlEngine::new(Arc::new(set_op_store()), EngineOpts::default());
        let samples = instant_vector(&engine, "up unless on (instance) target_info", 10_000).await;
        check!(samples.len() == 1);
        check!(samples[0].labels.get("__name__") == Some("up"));
        check!(samples[0].labels.get("instance") == Some("a"));
        check!(approx_eq(float_value(&samples[0].value), 1.0));
    }
}

mod vector_vector_arithmetic_combines_compatible_native_histograms {
    use super::*;

    #[derive(bon::Builder)]
    struct HistogramArithmeticExpectation {
        query: &'static str,
        count: f64,
        sum: f64,
    }

    #[tokio::test]
    pub(crate) async fn vector_vector_arithmetic_combines_compatible_native_histograms() {
        let mut left = native_histogram(4.0, 10.0);
        left.zero_count = 1.0;
        left.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 1,
        }];
        left.positive_counts = vec![3.0];
        let mut right = native_histogram(2.0, 4.0);
        right.zero_count = 0.5;
        right.positive_spans = left.positive_spans.clone();
        right.positive_counts = vec![1.5];

        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "a"),
                crate::test_support::TestLabelPair::new("job", "api"),
                crate::test_support::TestLabelPair::new("x", "1"),
            ]),
            10_000,
            left,
        );
        store.push_histogram(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "b"),
                crate::test_support::TestLabelPair::new("job", "api"),
                crate::test_support::TestLabelPair::new("x", "1"),
            ]),
            10_000,
            right,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for expectation in [
            HistogramArithmeticExpectation::builder()
                .query("a + on (x) b")
                .count(6.0)
                .sum(14.0)
                .build(),
            HistogramArithmeticExpectation::builder()
                .query("a - on (x) b")
                .count(2.0)
                .sum(6.0)
                .build(),
        ] {
            assert_on_x_histogram_stats(
                &engine,
                OnXHistogramStats {
                    query: expectation.query,
                    count: expectation.count,
                    sum: expectation.sum,
                },
            )
            .await;
        }
    }
}

mod vector_vector_arithmetic_drops_metadata_labels {
    use super::*;

    #[tokio::test]
    pub(crate) async fn vector_vector_arithmetic_drops_metadata_labels() {
        let mut store = InMemoryMetricStore::new();
        push_metadata_sample(&mut store, "a", 10.0);
        push_metadata_sample(&mut store, "b", 5.0);

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(&engine, "requests_total + 1", 10_000).await;
        check!(samples.len() == 2);
        for name in ["__name__", "__type__", "__unit__"] {
            check!(
                samples
                    .iter()
                    .all(|sample| sample.labels.get(name).is_none()),
                "{name} must be dropped"
            );
        }
    }
}

mod vector_vector_arithmetic_fill_uses_missing_side_values {
    use super::*;

    #[tokio::test]
    pub(crate) async fn vector_vector_arithmetic_fill_uses_missing_side_values() {
        let mut store = InMemoryMetricStore::new();
        for (metric, instance, value) in [
            ("a", "matched", 10.0),
            ("a", "left-only", 7.0),
            ("b", "matched", 3.0),
            ("b", "right-only", 5.0),
        ] {
            push_metric_instance_sample(&mut store, metric, instance, value);
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(&engine, "a + on (instance) fill(0) b", 10_000).await;
        let mut values = BTreeMap::new();
        for sample in &samples {
            values.insert(
                sample.labels.get("instance").expect("instance label"),
                float_value(&sample.value),
            );
        }
        assert2::assert!(values.len() == 3);
        check!(approx_eq(values["matched"], 13.0));
        check!(approx_eq(values["left-only"], 7.0));
        check!(approx_eq(values["right-only"], 5.0));
        check!(samples.iter().all(|sample| {
            let label_names = sample
                .labels
                .iter()
                .map(|label| label.name)
                .collect::<Vec<_>>();
            sample.labels.get("__name__").is_none() && label_names == vec!["instance"]
        }));
    }
}

mod vector_vector_arithmetic_matches_on_labels {
    use super::*;

    #[tokio::test]
    pub(crate) async fn vector_vector_arithmetic_matches_on_labels() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "a"),
                crate::test_support::TestLabelPair::new("x", "1"),
            ]),
            10_000,
            10.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "b"),
                crate::test_support::TestLabelPair::new("x", "1"),
            ]),
            10_000,
            5.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "b"),
                crate::test_support::TestLabelPair::new("x", "2"),
            ]),
            10_000,
            99.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(&engine, "a + on (x) b", 10_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "x",
                label_value: "1",
            },
            15.0,
        );
    }
}

mod vector_vector_arithmetic_scales_native_histograms_with_matched_floats {
    use super::*;

    #[derive(bon::Builder)]
    struct HistogramScaleExpectation {
        query: &'static str,
        count: f64,
        sum: f64,
    }

    #[tokio::test]
    pub(crate) async fn vector_vector_arithmetic_scales_native_histograms_with_matched_floats() {
        let histogram = single_bucket_native_histogram(4.0, 10.0, 1.0, 3.0);

        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "duration"),
                crate::test_support::TestLabelPair::new("job", "api"),
                crate::test_support::TestLabelPair::new("x", "1"),
            ]),
            10_000,
            histogram,
        );
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "factor"),
                crate::test_support::TestLabelPair::new("job", "api"),
                crate::test_support::TestLabelPair::new("x", "1"),
            ]),
            10_000,
            2.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for expectation in [
            HistogramScaleExpectation::builder()
                .query("duration * on (x) factor")
                .count(8.0)
                .sum(20.0)
                .build(),
            HistogramScaleExpectation::builder()
                .query("factor * on (x) duration")
                .count(8.0)
                .sum(20.0)
                .build(),
            HistogramScaleExpectation::builder()
                .query("duration / on (x) factor")
                .count(2.0)
                .sum(5.0)
                .build(),
        ] {
            assert_on_x_histogram_stats(
                &engine,
                OnXHistogramStats {
                    query: expectation.query,
                    count: expectation.count,
                    sum: expectation.sum,
                },
            )
            .await;
        }

        let samples =
            instant_vector(&engine, "histogram_count(factor / on (x) duration)", 10_000).await;
        assert2::assert!(samples.is_empty());
    }
}

mod vector_vector_comparison_matches_native_histogram_equality {
    use super::*;

    #[derive(bon::Builder)]
    struct HistogramFixture {
        name: &'static str,
        histogram: NativeHistogram,
    }

    #[tokio::test]
    pub(crate) async fn vector_vector_comparison_matches_native_histogram_equality() {
        let mut left = native_histogram(4.0, 10.0);
        left.zero_count = 1.0;
        left.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 1,
        }];
        left.positive_counts = vec![3.0];
        let equal = left.clone();
        let mut different = left.clone();
        different.sum = 11.0;

        let mut store = InMemoryMetricStore::new();
        for fixture in [
            HistogramFixture::builder()
                .name("a")
                .histogram(left)
                .build(),
            HistogramFixture::builder()
                .name("b")
                .histogram(equal)
                .build(),
            HistogramFixture::builder()
                .name("c")
                .histogram(different)
                .build(),
        ] {
            store.push_histogram(
                "tenant-a",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", fixture.name),
                    crate::test_support::TestLabelPair::new("job", "api"),
                    crate::test_support::TestLabelPair::new("x", "1"),
                ]),
                10_000,
                fixture.histogram,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for query in [
            "histogram_count(a == on (x) b)",
            "histogram_count(a != on (x) c)",
        ] {
            let samples = instant_vector(&engine, query, 10_000).await;
            assert2::assert!(samples.len() == 1, "{query}");
            assert2::assert!(
                samples[0].labels == labels(&[crate::test_support::TestLabelPair::new("x", "1")]),
                "{query}"
            );
            assert2::assert!(samples[0].value == SampleValue::Float(4.0), "{query}");
        }

        let samples = instant_vector(&engine, "a == on (x) c", 10_000).await;
        assert2::assert!(samples.is_empty());

        let samples = instant_vector(&engine, "a == bool on (x) c", 10_000).await;
        check!(samples.len() == 1);
        check!(samples[0].labels.get("__name__").is_none());
        check!(samples[0].labels.get("job").is_none());
        check!(samples[0].labels.get("x") == Some("1"));
        check!(approx_eq(float_value(&samples[0].value), 0.0));

        let samples = instant_vector(&engine, "a > bool on (x) b", 10_000).await;
        assert2::assert!(samples.is_empty());
    }
}

mod vector_vector_group_left_carries_labels_from_one_side {
    use super::*;

    #[tokio::test]
    pub(crate) async fn vector_vector_group_left_carries_labels_from_one_side() {
        let engine = region_info_engine(&[
            InstanceRequests {
                job: "api",
                instance: "a",
                total: 100.0,
            },
            InstanceRequests {
                job: "api",
                instance: "b",
                total: 50.0,
            },
        ]);
        let samples = instant_vector(
            &engine,
            "http_requests_total / on (job) group_left(region) target_info",
            10_000,
        )
        .await;
        assert2::assert!(samples.len() == 2);
        for sample in samples {
            check!(sample.labels.get("__name__").is_none());
            check!(sample.labels.get("job") == Some("api"));
            check!(sample.labels.get("region") == Some("east"));
        }
    }
}

mod vector_vector_group_left_fill_right_preserves_unmatched_many_side {
    use super::*;

    #[tokio::test]
    pub(crate) async fn vector_vector_group_left_fill_right_preserves_unmatched_many_side() {
        let engine = region_info_engine(&[
            InstanceRequests {
                job: "api",
                instance: "a",
                total: 100.0,
            },
            InstanceRequests {
                job: "api",
                instance: "b",
                total: 50.0,
            },
            InstanceRequests {
                job: "worker",
                instance: "c",
                total: 7.0,
            },
        ]);
        let samples = instant_vector(
            &engine,
            "http_requests_total + on (job) group_left(region) fill_right(0) target_info",
            10_000,
        )
        .await;
        assert_filled_many_side(&samples);
    }
}

mod vector_vector_group_right_carries_labels_from_one_side {
    use super::*;

    #[derive(bon::Builder)]
    struct InstanceSample {
        instance: &'static str,
        value: f64,
    }

    #[tokio::test]
    pub(crate) async fn vector_vector_group_right_carries_labels_from_one_side() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "target_limit"),
                crate::test_support::TestLabelPair::new("job", "api"),
                crate::test_support::TestLabelPair::new("region", "east"),
            ]),
            10_000,
            100.0,
        );
        for sample in [
            InstanceSample::builder().instance("a").value(10.0).build(),
            InstanceSample::builder().instance("b").value(25.0).build(),
        ] {
            push_named_job_instance_sample(
                &mut store,
                "http_requests_total",
                "api",
                sample.instance,
                sample.value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(
            &engine,
            "target_limit / on (job) group_right(region) http_requests_total",
            10_000,
        )
        .await;
        check!(samples.len() == 2);
        let has_east_api_sample = |instance: &str, want: f64| {
            samples.iter().any(|sample| {
                sample.labels.get("__name__").is_none()
                    && sample.labels.get("job") == Some("api")
                    && sample.labels.get("region") == Some("east")
                    && sample.labels.get("instance") == Some(instance)
                    && approx_eq(float_value(&sample.value), want)
            })
        };
        check!(has_east_api_sample("a", 10.0));
        check!(has_east_api_sample("b", 4.0));
    }
}

mod vector_vector_group_right_fill_left_preserves_unmatched_many_side {
    use super::*;

    #[derive(bon::Builder)]
    struct RequestSeries {
        job: &'static str,
        instance: &'static str,
        value: f64,
    }

    #[tokio::test]
    pub(crate) async fn vector_vector_group_right_fill_left_preserves_unmatched_many_side() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "job_quota"),
                crate::test_support::TestLabelPair::new("job", "api"),
                crate::test_support::TestLabelPair::new("region", "east"),
            ]),
            10_000,
            10.0,
        );
        for series in [
            RequestSeries::builder()
                .job("api")
                .instance("a")
                .value(100.0)
                .build(),
            RequestSeries::builder()
                .job("api")
                .instance("b")
                .value(50.0)
                .build(),
            RequestSeries::builder()
                .job("worker")
                .instance("c")
                .value(7.0)
                .build(),
        ] {
            push_named_job_instance_sample(
                &mut store,
                "http_requests_total",
                series.job,
                series.instance,
                series.value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        // Prometheus swaps the operand sides for group_right before applying fill flags.
        let samples = instant_vector(
            &engine,
            "job_quota + on (job) group_right(region) fill_right(0) http_requests_total",
            10_000,
        )
        .await;
        assert_filled_many_side(&samples);
    }
}

mod limit_ratio_over_bound_emits_capping_warning {
    use super::*;
    #[cfg(feature = "experimental-functions")]
    use super::*;

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    pub(crate) async fn limit_ratio_over_bound_emits_capping_warning() {
        let mut store = InMemoryMetricStore::new();
        for instance in ["0", "1"] {
            store.push_float(
                "tenant-a",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "http_requests"),
                    crate::test_support::TestLabelPair::new("instance", instance),
                ]),
                0,
                1.0,
            );
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let annotated = engine
            .query_instant_with_annotations(
                &tenant_id("tenant-a"),
                "count(limit_ratio(1.1, http_requests))",
                0,
            )
            .await
            .expect("query");
        let annotations = annotated.annotations;
        assert2::assert!(
            annotations.warnings
                == vec![
                    "PromQL warning: ratio value should be between -1 and 1, got 1.1, capping to 1"
                        .to_string()
                ]
        );
    }
}

mod quantile_out_of_range_phi_returns_signed_inf_with_warning {
    use super::*;

    struct QuantileSample {
        instance: &'static str,
        value: f64,
    }

    struct QuantileExpectation {
        query: &'static str,
        phi_text: &'static str,
        predicate: fn(f64) -> bool,
    }

    /// M18: an out-of-range or NaN `quantile` phi does not error.
    ///
    /// The aggregate matches Prometheus and the `histogram_quantile` family in this
    /// file. It returns `+Inf` for phi > 1, `-Inf` for phi < 0, and `NaN` for a NaN
    /// phi. It also raises an `InvalidQuantileWarning`, and it never aborts.
    #[tokio::test]
    pub(crate) async fn quantile_out_of_range_phi_returns_signed_inf_with_warning() {
        let mut store = InMemoryMetricStore::new();
        let samples = [
            QuantileSample {
                instance: "0",
                value: 1.0,
            },
            QuantileSample {
                instance: "1",
                value: 2.0,
            },
            QuantileSample {
                instance: "2",
                value: 3.0,
            },
        ];
        for sample_index in 0..samples.len() {
            let sample = &samples[sample_index];
            let lbls = labels(&[
                crate::test_support::TestLabelPair::new("__name__", "m"),
                crate::test_support::TestLabelPair::new("instance", sample.instance),
            ]);
            store.push_float("t", lbls, 120_000, sample.value);
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let time_ms = 120_000_i64;

        let queries = [
            QuantileExpectation {
                query: "quantile(1.1, m)",
                phi_text: "1.1",
                predicate: f64::is_infinite,
            },
            QuantileExpectation {
                query: "quantile(-0.1, m)",
                phi_text: "-0.1",
                predicate: f64::is_infinite,
            },
            QuantileExpectation {
                query: "quantile(NaN, m)",
                phi_text: "NaN",
                predicate: f64::is_nan,
            },
        ];
        for query_index in 0..queries.len() {
            let case = &queries[query_index];
            let annotated = engine
                .query_instant_with_annotations(&tenant_id("t"), case.query, time_ms)
                .await
                .unwrap_or_else(|error| panic!("`{}` must NOT error: {error}", case.query));

            let QueryResult::InstantVector(samples) = annotated.result else {
                panic!("`{}` must yield an instant vector", case.query);
            };
            assert2::assert!(samples.len() == 1);
            let value = float_value(&samples[0].value);
            assert2::assert!((case.predicate)(value));
            // For the +/-Inf cases, also pin the sign.
            if case.query.contains("1.1") {
                assert2::assert!(value > 0.0);
            } else if case.query.contains("-0.1") {
                assert2::assert!(value < 0.0);
            }

            assert2::assert!(
                annotated.annotations.warnings
                    == vec![format!(
                        "PromQL warning: quantile value should be between 0 and 1, got {} (1:10)",
                        case.phi_text
                    )]
            );
        }
    }
}

mod topk_refuses_a_k_that_does_not_fit_an_int64 {
    use super::*;

    /// Prometheus bounds `k` to what an `int64` holds: `topk` refuses any parameter
    /// greater than or equal to `maxInt64`, which is `2^63 - 1024` and the last
    /// `f64` below `2^63`. A `usize` does not impose that bound on a 64-bit target
    /// -- it reaches almost twice as far -- so the bound has to be spelled out.
    #[tokio::test]
    pub(crate) async fn topk_refuses_a_k_that_does_not_fit_an_int64() {
        let engine = memory_bytes_engine();

        // `2^63` and the bound itself are both refused; `1e19` is the value that a
        // `u64` would have swallowed whole.
        for query in [
            "topk(1e19, memory_bytes)",
            "topk(9223372036854774784, memory_bytes)",
        ] {
            let error = engine
                .query_instant(&tenant_id("tenant-a"), query, 10_000)
                .await
                .unwrap_err();

            check!(matches!(error, PromqlError::Plan(_)), "{query}");
            check!(format!("{error}").contains("overflows int64"), "{query}");
        }

        // The next `f64` below the bound is `2^63 - 2048`, which is accepted and
        // selects everything.
        let result = engine
            .query_instant(
                &tenant_id("tenant-a"),
                "topk(9223372036854773760, memory_bytes)",
                10_000,
            )
            .await
            .expect("a k just inside the int64 bound");
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected a vector");
        };
        let projection = topk_projection(&samples);
        assert2::assert!(projection.len() == 3);
        assert2::assert!(projection[0].instance == Some("a"));
        assert2::assert!(projection[0].value == 1.0);
        assert2::assert!(projection[1].instance == Some("b"));
        assert2::assert!(projection[1].value == 3.0);
        assert2::assert!(projection[2].instance == Some("c"));
        assert2::assert!(projection[2].value == 2.0);
    }
}
