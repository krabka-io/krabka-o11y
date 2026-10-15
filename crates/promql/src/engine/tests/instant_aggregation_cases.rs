use super::*;

mod instant_avg_aggregate_does_not_overflow {
    use super::*;

    /// M17: `avg` of large-magnitude samples must not overflow the running sum.
    ///
    /// The running sum must not reach +Inf or -Inf. The incremental Kahan mean
    /// stays finite and equals the common value for two equal maxima.
    #[tokio::test]
    pub(crate) async fn instant_avg_aggregate_does_not_overflow() {
        let mut store = InMemoryMetricStore::new();
        for instance in 0..2 {
            store.push_float(
                "tenant-a",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "huge"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                    crate::test_support::TestLabelPair::new("instance", &instance.to_string()),
                ]),
                10_000,
                f64::MAX,
            );
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let QueryResult::InstantVector(avg) = engine
            .query_instant(&tenant_id("tenant-a"), "avg(huge)", 10_000)
            .await
            .unwrap()
        else {
            panic!("expected vector");
        };
        assert2::assert!(avg.len() == 1);
        let value = float_value(&avg[0].value);
        assert2::assert!(value.is_finite());
        assert2::assert!(approx_eq(value, f64::MAX));
    }
}

mod instant_bottomk_keeps_smallest_samples_with_original_labels {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_bottomk_keeps_smallest_samples_with_original_labels() {
        let engine = memory_bytes_engine();
        let samples = instant_vector(&engine, "bottomk(2, memory_bytes)", 10_000).await;
        check!(samples.len() == 2);
        check!(samples.iter().any(|sample| {
            sample.labels.get("__name__") == Some("memory_bytes")
                && sample.labels.get("instance") == Some("a")
                && approx_eq(float_value(&sample.value), 1.0)
        }));
        check!(samples.iter().any(|sample| {
            sample.labels.get("__name__") == Some("memory_bytes")
                && sample.labels.get("instance") == Some("c")
                && approx_eq(float_value(&sample.value), 2.0)
        }));
    }
}

mod instant_bottomk_without_selects_smallest_sample_per_group_with_original_labels {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_bottomk_without_selects_smallest_sample_per_group_with_original_labels()
     {
        let engine = job_memory_bytes_engine(&[
            JobInstanceSample {
                job: "api",
                instance: "a",
                value: 4.0,
            },
            JobInstanceSample {
                job: "api",
                instance: "b",
                value: 1.0,
            },
            JobInstanceSample {
                job: "worker",
                instance: "c",
                value: 5.0,
            },
            JobInstanceSample {
                job: "worker",
                instance: "d",
                value: 2.0,
            },
        ]);
        let samples = instant_vector(
            &engine,
            "bottomk without (instance) (1, memory_bytes)",
            10_000,
        )
        .await;
        check!(samples.len() == 2);
        check!(has_job_memory_bytes_sample(
            &samples,
            &JobInstanceSample {
                job: "api",
                instance: "b",
                value: 1.0,
            },
        ));
        check!(has_job_memory_bytes_sample(
            &samples,
            &JobInstanceSample {
                job: "worker",
                instance: "d",
                value: 2.0,
            },
        ));
    }
}

mod instant_changes_counts_value_transitions_in_range {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_changes_counts_value_transitions_in_range() {
        let store = queue_depth_store(&[
            (0, 1.0),
            (60_000, 1.0),
            (120_000, 2.0),
            (180_000, 2.0),
            (240_000, 5.0),
        ]);

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(&engine, "changes(queue_depth[4m])", 240_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            2.0,
        );
    }
}

mod instant_count_counts_series {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_count_counts_series() {
        let engine = FloatStore::default()
            .up_job_sample("api", 1.0)
            .up_job_sample("web", 0.0)
            .engine();
        let samples = instant_vector(&engine, "count(up)", 10_000).await;
        assert2::assert!(samples.len() == 1);
        assert2::assert!(approx_eq(float_value(&samples[0].value), 2.0));
    }
}

mod instant_count_values_counts_by_sample_value {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_count_values_counts_by_sample_value() {
        let mut store = InMemoryMetricStore::new();
        for InstanceValueCase { instance, value } in [
            InstanceValueCase {
                instance: "0",
                value: 200.0,
            },
            InstanceValueCase {
                instance: "1",
                value: 200.0,
            },
            InstanceValueCase {
                instance: "2",
                value: 500.0,
            },
        ] {
            push_named_job_instance_sample(
                &mut store,
                "http_responses_total",
                "api",
                instance,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                &tenant_id("tenant-a"),
                r#"count_values("code", http_responses_total)"#,
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert2::assert!(samples.len() == 2);
        let ok = samples
            .iter()
            .find(|sample| sample.labels.get("code") == Some("200"))
            .expect("200 bucket");
        assert2::assert!(ok.labels.get("__name__") == None);
        assert2::assert!(approx_eq(float_value(&ok.value), 2.0));
        let err = samples
            .iter()
            .find(|sample| sample.labels.get("code") == Some("500"))
            .expect("500 bucket");
        assert2::assert!(approx_eq(float_value(&err.value), 1.0));
    }
}

mod instant_count_values_counts_native_histogram_sample_values {
    use super::{native_histogram_store::push_request_duration_instance_histogram, *};

    #[derive(bon::Builder)]
    struct HistogramInstance {
        instance: &'static str,
        histogram: NativeHistogram,
    }

    #[tokio::test]
    pub(crate) async fn instant_count_values_counts_native_histogram_sample_values() {
        let mut repeated = native_histogram(4.0, 10.0);
        repeated.zero_count = 1.0;
        repeated.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 1,
        }];
        repeated.positive_counts = vec![3.0];
        let mut distinct = repeated.clone();
        distinct.sum = 12.0;

        let mut store = InMemoryMetricStore::new();
        for sample in [
            HistogramInstance::builder()
                .instance("a")
                .histogram(repeated.clone())
                .build(),
            HistogramInstance::builder()
                .instance("b")
                .histogram(repeated.clone())
                .build(),
            HistogramInstance::builder()
                .instance("c")
                .histogram(distinct)
                .build(),
        ] {
            push_request_duration_instance_histogram(&mut store, sample.instance, sample.histogram);
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                &tenant_id("tenant-a"),
                r#"count_values by (job) ("histogram", request_duration_seconds)"#,
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        let mut values = samples
            .iter()
            .map(|sample| float_value(&sample.value))
            .collect::<Vec<_>>();
        values.sort_by(f64::total_cmp);
        check!(samples.len() == 2);
        check!(samples.iter().all(|sample| {
            sample.labels.get("__name__").is_none()
                && sample.labels.get("job") == Some("api")
                && sample.labels.get("histogram").is_some()
        }));
        check!(values == vec![1.0, 2.0]);
    }
}

mod instant_count_values_formats_infinity_as_prometheus {
    use super::*;

    /// M19: `count_values` formats a non-finite value with the Prometheus formatter.
    ///
    /// `count_values` uses the canonical Prometheus float formatter, so the label
    /// value is `+Inf`, not the `inf` that `f64::to_string` returns.
    #[tokio::test]
    pub(crate) async fn instant_count_values_formats_infinity_as_prometheus() {
        let mut store = InMemoryMetricStore::new();
        for instance in 0..2 {
            store.push_float(
                "tenant-a",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "ratio"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                    crate::test_support::TestLabelPair::new("instance", &instance.to_string()),
                ]),
                10_000,
                f64::INFINITY,
            );
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let QueryResult::InstantVector(samples) = engine
            .query_instant(
                &tenant_id("tenant-a"),
                r#"count_values("v", ratio)"#,
                10_000,
            )
            .await
            .unwrap()
        else {
            panic!("expected vector");
        };
        check!(samples.len() == 1);
        check!(
            samples[0].labels.get("v") == Some("+Inf"),
            "count_values must render +Inf, got {:?}",
            samples[0].labels.get("v")
        );
        check!(approx_eq(float_value(&samples[0].value), 2.0));
    }
}

mod instant_group_returns_one_for_each_group {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_group_returns_one_for_each_group() {
        let mut store = InMemoryMetricStore::new();
        push_metric_job_samples(
            &mut store,
            [
                ("up", "api", 10.0),
                ("up", "api", 30.0),
                ("up", "web", 99.0),
            ],
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(&engine, "group by (job) (up)", 10_000).await;
        assert2::assert!(samples.len() == 2);
        for sample in samples {
            assert2::assert!(sample.labels.get("__name__") == None);
            assert2::assert!(approx_eq(float_value(&sample.value), 1.0));
        }
    }
}

mod instant_increase_corrects_counter_resets {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_increase_corrects_counter_resets() {
        let store = SeriesFixture::new(http_requests_counter_labels())
            .at(0_i64, 1.0)
            .at(60_000, 2.0)
            .at(120_000, 1.0)
            .store();

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(&engine, "increase(http_requests_total[2m])", 120_000).await;
        assert2::assert!(samples.len() == 1);
        assert2::assert!(approx_eq(float_value(&samples[0].value), 2.0));
    }
}

mod instant_label_replace_uses_regex_capture_groups {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_label_replace_uses_regex_capture_groups() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("instance", "api-1:9100"),
            ]),
            10_000,
            1.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        assert_label_transform(
            &engine,
            r#"label_replace(up, "host", "$1", "instance", "([^:]+):.*")"#,
            "host",
            "api-1",
            "instance",
            "api-1:9100",
        )
        .await;
    }
}

mod instant_limit_ratio_negative_selects_complement_subset {
    #[cfg(feature = "experimental-functions")]
    use super::*;

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    pub(crate) async fn instant_limit_ratio_negative_selects_complement_subset() {
        let selected = selected_memory_instances("limit_ratio(-0.25, memory_bytes)").await;
        assert2::assert!(selected == vec!["e"]);
    }
}

mod instant_limit_ratio_selects_deterministic_hash_subset {
    #[cfg(feature = "experimental-functions")]
    use super::*;

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    pub(crate) async fn instant_limit_ratio_selects_deterministic_hash_subset() {
        let selected = selected_memory_instances("limit_ratio(0.75, memory_bytes)").await;
        assert2::assert!(selected == vec!["a", "b", "c", "d"]);
    }
}

mod instant_limitk_by_selects_deterministic_hash_subset_per_group {
    #[cfg(feature = "experimental-functions")]
    use super::*;

    #[cfg(feature = "experimental-functions")]
    struct SeriesInput {
        job: &'static str,
        instance: &'static str,
        value: f64,
    }

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    pub(crate) async fn instant_limitk_by_selects_deterministic_hash_subset_per_group() {
        let mut store = InMemoryMetricStore::new();
        for series in [
            SeriesInput {
                job: "api",
                instance: "a",
                value: 1.0,
            },
            SeriesInput {
                job: "api",
                instance: "b",
                value: 2.0,
            },
            SeriesInput {
                job: "api",
                instance: "c",
                value: 3.0,
            },
            SeriesInput {
                job: "worker",
                instance: "d",
                value: 4.0,
            },
            SeriesInput {
                job: "worker",
                instance: "e",
                value: 5.0,
            },
            SeriesInput {
                job: "worker",
                instance: "f",
                value: 6.0,
            },
        ] {
            store.push_float(
                "tenant-a",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "memory_bytes"),
                    crate::test_support::TestLabelPair::new("job", series.job),
                    crate::test_support::TestLabelPair::new("instance", series.instance),
                ]),
                10_000,
                series.value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(&engine, "limitk by (job) (1, memory_bytes)", 10_000).await;
        check!(samples.len() == 2);
        check!(samples.iter().any(|sample| {
            sample.labels.get("__name__") == Some("memory_bytes")
                && sample.labels.get("job") == Some("api")
                && sample.labels.get("instance") == Some("c")
                && approx_eq(float_value(&sample.value), 3.0)
        }));
        let selected = sample_instances(&samples);
        check!(selected == vec!["c", "e"]);
    }
}

mod instant_limitk_selects_deterministic_hash_subset {
    #[cfg(feature = "experimental-functions")]
    use super::*;

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    pub(crate) async fn instant_limitk_selects_deterministic_hash_subset() {
        let selected = selected_memory_instances("limitk(2, memory_bytes)").await;
        assert2::assert!(selected == vec!["d", "e"]);
    }
}

mod instant_quantile_aggregation_ignores_histograms {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_quantile_aggregation_ignores_histograms() {
        let store = mixed_metric_store(
            "latency_seconds",
            &[
                MixedMetricFloatSample::builder()
                    .instance("a")
                    .value(2.0)
                    .build(),
                MixedMetricFloatSample::builder()
                    .instance("b")
                    .value(6.0)
                    .build(),
            ],
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples =
            instant_vector(&engine, "quantile by (job) (0.5, latency_seconds)", 10_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            4.0,
        );
    }
}

mod instant_quantile_interpolates_per_group {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_quantile_interpolates_per_group() {
        let engine = api_latency_engine(&[1.0, 2.0, 4.0, 8.0]);
        let samples =
            instant_vector(&engine, "quantile by (job) (0.5, latency_seconds)", 10_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            3.0,
        );
    }
}

mod instant_rate_extrapolates_counter_window {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_rate_extrapolates_counter_window() {
        let engine = requests_counter_engine(4);
        let samples = instant_vector(&engine, "rate(http_requests_total[5m])", 300_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            5.0 / 300.0,
        );
    }
}

mod instant_resets_counts_counter_decreases_in_range {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_resets_counts_counter_decreases_in_range() {
        let store = SeriesFixture::new(http_requests_counter_labels())
            .at(0_i64, 0.0)
            .at(60_000, 5.0)
            .at(120_000, 1.0)
            .at(180_000, 4.0)
            .at(240_000, 2.0)
            .store();

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(&engine, "resets(http_requests_total[4m])", 240_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            2.0,
        );
    }
}

mod instant_stddev_and_stdvar_aggregate_population_variance {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_stddev_and_stdvar_aggregate_population_variance() {
        let engine = api_latency_engine(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
        let stdvar_samples = instant_vector(&engine, "stdvar(latency_seconds)", 10_000).await;
        let stddev_samples = instant_vector(&engine, "stddev(latency_seconds)", 10_000).await;
        check!(stdvar_samples.len() == 1);
        check!(stddev_samples.len() == 1);
        check!(approx_eq(float_value(&stdvar_samples[0].value), 4.0));
        check!(approx_eq(float_value(&stddev_samples[0].value), 2.0));
    }
}

mod instant_stdvar_aggregate_is_stable_for_large_offset_group {
    use super::*;

    /// M16: `stdvar` and `stddev` must not cancel into a negative variance.
    ///
    /// The group has a large offset and close values. A negative variance gives
    /// NaN from `sqrt`. Welford returns the small positive population variance for
    /// `{0,1,2}` -> 2/3.
    #[tokio::test]
    pub(crate) async fn instant_stdvar_aggregate_is_stable_for_large_offset_group() {
        let mut store = InMemoryMetricStore::new();
        for InstanceValueCase { instance, value } in [
            InstanceValueCase {
                instance: "0",
                value: 1e8,
            },
            InstanceValueCase {
                instance: "1",
                value: 1e8 + 1.0,
            },
            InstanceValueCase {
                instance: "2",
                value: 1e8 + 2.0,
            },
        ] {
            push_named_job_instance_sample(&mut store, "big", "api", instance, value);
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let QueryResult::InstantVector(stdvar) = engine
            .query_instant(&tenant_id("tenant-a"), "stdvar(big)", 10_000)
            .await
            .unwrap()
        else {
            panic!("expected vector");
        };
        assert2::assert!(stdvar.len() == 1);
        let value = float_value(&stdvar[0].value);
        check!(!value.is_nan(), "stdvar must be finite, got NaN");
        check!(value > 0.0, "stdvar must be positive, got {value}");
        check!(approx_eq(value, 2.0 / 3.0), "stdvar == 2/3, got {value}");
    }
}

mod instant_sum_aggregates_all_series {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_sum_aggregates_all_series() {
        let engine = FloatStore::default()
            .up_job_sample("api", 1.0)
            .up_job_sample("web", 2.0)
            .engine();
        let samples = instant_vector(&engine, "sum(up)", 10_000).await;
        check!(samples.len() == 1);
        check!(samples[0].labels.is_empty());
        check!(approx_eq(float_value(&samples[0].value), 3.0));
    }
}

mod instant_sum_by_groups_by_exact_labels_and_drops_metric_name {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_sum_by_groups_by_exact_labels_and_drops_metric_name() {
        let engine = FloatStore::default()
            .up_sample("api", "a", 1.0)
            .up_sample("api", "b", 2.0)
            .up_sample("web", "c", 4.0)
            .engine();
        let samples = instant_vector(&engine, "sum by (job) (up)", 10_000).await;
        assert2::assert!(samples.len() == 2);
        let api = samples
            .iter()
            .find(|sample| sample.labels.get("job") == Some("api"))
            .expect("api group");
        assert2::assert!(api.labels.get("__name__") == None);
        assert2::assert!(api.labels.get("instance") == None);
        assert2::assert!(approx_eq(float_value(&api.value), 3.0));
        let web = samples
            .iter()
            .find(|sample| sample.labels.get("job") == Some("web"))
            .expect("web group");
        assert2::assert!(approx_eq(float_value(&web.value), 4.0));
    }
}

mod instant_topk_and_bottomk_rank_nan_last {
    use super::*;

    #[derive(bon::Builder)]
    struct RankInput {
        instance: &'static str,
        value: f64,
    }

    #[derive(Debug, Eq, PartialEq)]
    struct RankedInstance {
        instance: String,
        value: Option<OrderedF64>,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct OrderedF64(u64);

    struct TopKCase {
        query: &'static str,
        expected: Vec<RankedInstance>,
    }

    /// Prometheus ranks a `NaN` below every number under both operators. `topk`
    /// and `bottomk` therefore select the `NaN` series only after they select
    /// every number. `None` stands for the `NaN` sample in the table below,
    /// because no float compares equal to a `NaN`.
    #[tokio::test]
    pub(crate) async fn instant_topk_and_bottomk_rank_nan_last() {
        let mut store = InMemoryMetricStore::new();
        for input in [
            RankInput::builder().instance("a").value(1.0).build(),
            RankInput::builder().instance("c").value(2.0).build(),
            RankInput::builder().instance("n").value(f64::NAN).build(),
        ] {
            store.push_float(
                "tenant-a",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "memory_bytes"),
                    crate::test_support::TestLabelPair::new("instance", input.instance),
                ]),
                10_000,
                input.value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for case in [
            TopKCase {
                query: "topk(1, memory_bytes)",
                expected: vec![ranked("c", Some(2.0))],
            },
            TopKCase {
                query: "bottomk(1, memory_bytes)",
                expected: vec![ranked("a", Some(1.0))],
            },
            TopKCase {
                query: "topk(2, memory_bytes)",
                expected: vec![ranked("c", Some(2.0)), ranked("a", Some(1.0))],
            },
            TopKCase {
                query: "bottomk(2, memory_bytes)",
                expected: vec![ranked("a", Some(1.0)), ranked("c", Some(2.0))],
            },
            TopKCase {
                query: "topk(3, memory_bytes)",
                expected: vec![
                    ranked("c", Some(2.0)),
                    ranked("a", Some(1.0)),
                    ranked("n", None),
                ],
            },
            TopKCase {
                query: "bottomk(3, memory_bytes)",
                expected: vec![
                    ranked("a", Some(1.0)),
                    ranked("c", Some(2.0)),
                    ranked("n", None),
                ],
            },
        ] {
            let samples = instant_vector(&engine, case.query, 10_000).await;
            let selected = samples
                .iter()
                .map(|sample| {
                    let value = float_value(&sample.value);
                    RankedInstance {
                        instance: sample.labels.get("instance").unwrap().to_owned(),
                        value: (!value.is_nan())
                            .then_some(value)
                            .map(|value| OrderedF64(value.to_bits())),
                    }
                })
                .collect::<Vec<_>>();
            assert2::assert!(selected == case.expected);
        }
    }

    fn ranked(instance: &'static str, value: Option<f64>) -> RankedInstance {
        RankedInstance {
            instance: instance.to_owned(),
            value: value.map(|value| OrderedF64(value.to_bits())),
        }
    }
}

mod instant_topk_by_selects_largest_sample_per_group_with_original_labels {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_topk_by_selects_largest_sample_per_group_with_original_labels() {
        let engine = job_memory_bytes_engine(&[
            JobInstanceSample {
                job: "api",
                instance: "a",
                value: 1.0,
            },
            JobInstanceSample {
                job: "api",
                instance: "b",
                value: 3.0,
            },
            JobInstanceSample {
                job: "worker",
                instance: "c",
                value: 5.0,
            },
            JobInstanceSample {
                job: "worker",
                instance: "d",
                value: 2.0,
            },
        ]);
        let samples = instant_vector(&engine, "topk by (job) (1, memory_bytes)", 10_000).await;
        check!(samples.len() == 2);
        check!(has_job_memory_bytes_sample(
            &samples,
            &JobInstanceSample {
                job: "api",
                instance: "b",
                value: 3.0,
            },
        ));
        check!(has_job_memory_bytes_sample(
            &samples,
            &JobInstanceSample {
                job: "worker",
                instance: "c",
                value: 5.0,
            },
        ));
    }
}

mod instant_topk_keeps_largest_samples_with_original_labels {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_topk_keeps_largest_samples_with_original_labels() {
        let engine = memory_bytes_engine();
        let samples = instant_vector(&engine, "topk(2, memory_bytes)", 10_000).await;
        let projection = topk_projection(&samples);
        assert2::assert!(projection.len() == 2);
        assert2::assert!(projection[0].metric_name == Some("memory_bytes"));
        assert2::assert!(projection[0].instance == Some("b"));
        assert2::assert!(projection[0].value == 3.0);
        assert2::assert!(projection[1].metric_name == Some("memory_bytes"));
        assert2::assert!(projection[1].instance == Some("c"));
        assert2::assert!(projection[1].value == 2.0);
    }
}
