use super::*;

mod instant_absent_over_time_returns_one_when_range_is_empty {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_absent_over_time_returns_one_when_range_is_empty() {
        let engine = FloatStore::default().up_job_sample("api", 1.0).engine();
        let samples =
            instant_vector(&engine, r#"absent_over_time(up{job="api"}[1m])"#, 120_000).await;
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

mod instant_absent_over_time_treats_native_histograms_as_present {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_absent_over_time_treats_native_histograms_as_present() {
        let store = store_with_request_duration_histogram_at(native_histogram(4.0, 10.0), 90_000);

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                &tenant_id("tenant-a"),
                r#"absent_over_time(request_duration_seconds{job="api"}[1m])"#,
                120_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert2::assert!(samples.is_empty());
    }
}

mod instant_absent_over_time_with_or_matchers_returns_unlabeled_absence_sample {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_absent_over_time_with_or_matchers_returns_unlabeled_absence_sample()
    {
        assert_lone_unlabeled_value_at(
            &FloatStore::default().engine(),
            LoneValueQuery {
                query: r#"absent_over_time(up{job="api" or job="web"}[1m])"#,
                time_ms: 120_000,
                want: 1.0,
            },
        )
        .await;
    }
}

mod instant_absent_returns_one_with_equality_matcher_labels_when_vector_is_empty {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_absent_returns_one_with_equality_matcher_labels_when_vector_is_empty()
     {
        let engine = FloatStore::default().up_sample("api", "a", 1.0).engine();
        let result = engine
            .query_instant(
                &tenant_id("tenant-a"),
                r#"absent(up{job="worker",instance=~".*"})"#,
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        check!(samples.len() == 1);
        check!(samples[0].labels.get("__name__").is_none());
        check!(samples[0].labels.get("job") == Some("worker"));
        check!(samples[0].labels.get("instance").is_none());
        check!(approx_eq(float_value(&samples[0].value), 1.0));
    }
}

mod instant_absent_with_or_matchers_returns_unlabeled_absence_sample {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_absent_with_or_matchers_returns_unlabeled_absence_sample() {
        assert_lone_unlabeled_value_at(
            &FloatStore::default().engine(),
            LoneValueQuery {
                query: r#"absent(up{job="api" or job="web"})"#,
                time_ms: 10_000,
                want: 1.0,
            },
        )
        .await;
    }
}

mod instant_selector_at_and_offset_combine_order_independently {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_selector_at_and_offset_combine_order_independently() {
        let engine = short_lookback_up_engine();
        for query in ["up @ 120 offset 1m", "up offset 1m @ 120"] {
            let samples = instant_vector(&engine, query, 999_000).await;
            check!(samples.len() == 1);
            check!(samples[0].ts_ms == 999_000);
            check!(approx_eq(float_value(&samples[0].value), 1.0));
        }
    }
}

mod instant_selector_at_uses_absolute_evaluation_time {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_selector_at_uses_absolute_evaluation_time() {
        let engine = short_lookback_up_engine();
        let samples = instant_vector(&engine, "up @ 60", 120_000).await;
        check!(samples.len() == 1);
        check!(samples[0].ts_ms == 120_000);
        check!(approx_eq(float_value(&samples[0].value), 1.0));
    }
}

mod instant_selector_honors_label_matchers_and_tenant {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_selector_honors_label_matchers_and_tenant() {
        let engine = FloatStore::default()
            .up_job_sample("api", 1.0)
            .up_job_sample("web", 0.0)
            .tenant_sample(
                "tenant-b",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "up"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                ]),
                9.0,
            )
            .engine();
        let samples = instant_vector(&engine, r#"up{job=~"a.*"}"#, 10_000).await;
        check!(samples.len() == 1);
        check!(samples[0].labels.get("job") == Some("api"));
        check!(approx_eq(float_value(&samples[0].value), 1.0));
    }
}

mod instant_selector_offset_shifts_evaluation_time_backwards {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_selector_offset_shifts_evaluation_time_backwards() {
        let engine = short_lookback_up_engine();
        let samples = instant_vector(&engine, "up offset 1m", 120_000).await;
        check!(samples.len() == 1);
        check!(samples[0].ts_ms == 120_000);
        check!(approx_eq(float_value(&samples[0].value), 1.0));
    }
}

mod instant_selector_or_matchers_union_matching_series {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_selector_or_matchers_union_matching_series() {
        let engine = FloatStore::default()
            .up_sample("api", "a", 1.0)
            .up_sample("web", "b", 2.0)
            .up_sample("db", "c", 3.0)
            .engine();
        let samples = instant_vector(&engine, r#"up{job="api" or job="web"}"#, 10_000).await;
        assert2::assert!(samples.len() == 2);
        let mut values_by_job = BTreeMap::new();
        for sample in &samples {
            values_by_job.insert(
                sample.labels.get("job").expect("job label").to_string(),
                float_value(&sample.value),
            );
        }
        check!(approx_eq(values_by_job["api"], 1.0));
        check!(approx_eq(values_by_job["web"], 2.0));
        check!(!values_by_job.contains_key("db"));
    }
}

mod instant_selector_planner_path_matches_interpreter {
    use super::*;

    #[derive(bon::Builder)]
    struct SelectorSample {
        labels: Labels,
        timestamp_ms: i64,
        value: f64,
    }

    impl FloatSampleSeed for SelectorSample {
        fn push_into(self, store: &mut InMemoryMetricStore) {
            store.push_float("t", self.labels, self.timestamp_ms, self.value);
        }
    }

    struct SelectorCase {
        query: &'static str,
        timestamp_ms: i64,
    }

    fn up_sample(job: &str, timestamp_ms: i64, value: f64) -> SelectorSample {
        SelectorSample::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", job),
            ]))
            .timestamp_ms(timestamp_ms)
            .value(value)
            .build()
    }

    #[tokio::test]
    pub(crate) async fn instant_selector_planner_path_matches_interpreter() {
        use promql_parser::parser::Expr;

        use crate::{DurationExprContext, parse_promql_with_duration_context};

        // A small float-only store with multiple series, an empty-string-ish
        // label set, an offset-relevant history, a stale marker (job=db: its
        // latest in-window sample is a stale-NaN marker, so it must be DROPPED
        // on both paths), and a genuine-NaN series (job=nan: its latest
        // in-window sample is a genuine NaN, so it must be KEPT as a NaN value
        // on both paths).
        let mut store = InMemoryMetricStore::new();
        let stale_bits = stale_nan();
        push_float_samples(
            &mut store,
            [
                up_sample("api", 0, 1.0),
                up_sample("api", 60_000, 2.0),
                up_sample("api", 120_000, 3.0),
                up_sample("db", 60_000, 9.0),
                up_sample("db", 120_000, stale_bits),
                // Genuine NaN as the latest in-window sample: kept as a NaN value.
                up_sample("nan", 60_000, 5.0),
                up_sample("nan", 120_000, f64::NAN),
                SelectorSample::builder()
                    .labels(labels(&[
                        crate::test_support::TestLabelPair::new("__name__", "down"),
                        crate::test_support::TestLabelPair::new("job", "api"),
                    ]))
                    .timestamp_ms(120_000)
                    .value(7.0)
                    .build(),
                SelectorSample::builder()
                    .labels(labels(&[crate::test_support::TestLabelPair::new(
                        "__name__", "lonely",
                    )]))
                    .timestamp_ms(120_000)
                    .value(42.0)
                    .build(),
            ],
        );
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let selectors = [
            SelectorCase {
                query: "up",
                timestamp_ms: 120_000,
            },
            SelectorCase {
                query: "up{job=\"api\"}",
                timestamp_ms: 120_000,
            },
            SelectorCase {
                query: "up{job=~\"a.*\"}",
                timestamp_ms: 120_000,
            },
            SelectorCase {
                query: "up{job!=\"api\"}",
                timestamp_ms: 120_000,
            },
            SelectorCase {
                query: "{__name__=~\".+\"}",
                timestamp_ms: 120_000,
            },
            SelectorCase {
                query: "up offset 1m",
                timestamp_ms: 120_000,
            },
            SelectorCase {
                query: "up @ 60",
                timestamp_ms: 120_000,
            },
            SelectorCase {
                query: "up{job=\"missing\"}",
                timestamp_ms: 120_000,
            },
            SelectorCase {
                query: "lonely",
                timestamp_ms: 120_000,
            },
            // Genuine NaN must be kept (NaN value) on both paths.
            SelectorCase {
                query: "up{job=\"nan\"}",
                timestamp_ms: 120_000,
            },
            // Stale-NaN marker must be dropped (empty result) on both paths.
            SelectorCase {
                query: "up{job=\"db\"}",
                timestamp_ms: 120_000,
            },
        ];

        for case in selectors {
            let query = case.query;
            let time_ms = case.timestamp_ms;
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
            let Expr::VectorSelector(selector) = expr else {
                panic!("`{query}` did not parse to a bare vector selector");
            };

            let interpreter = engine
                .eval_instant_selector("t", &selector, time_ms)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
            let planner = engine
                .eval_instant_selector_via_planner("t", &selector, time_ms)
                .await
                .unwrap_or_else(|error| panic!("planner `{query}`: {error}"));

            let normalize = |result: QueryResult| fingerprint_sorted(result, query);

            let interpreter = normalize(interpreter);
            let planner = normalize(planner);
            assert2::assert!(instant_samples_match(&interpreter, &planner));

            // Pin the staleness semantics the parity above relies on.
            if query == "up{job=\"nan\"}" {
                // Genuine NaN is kept as a NaN value (and is not a stale marker).
                assert2::assert!(planner.len() == 1);
                let value = float_value(&planner[0].value);
                assert2::assert!(value.is_nan());
                assert2::assert!(!super::super::super::is_stale_nan(value));
            }
            if query == "up{job=\"db\"}" {
                // Stale-NaN marker terminates the series: empty result.
                assert2::assert!(planner.is_empty());
            }
        }
    }
}

mod instant_selector_returns_latest_sample_within_lookback {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_selector_returns_latest_sample_within_lookback() {
        let store = up_series_store(&[(10_000, 1.0), (20_000, 2.0), (40_000, 4.0)]);
        let engine = lookback_engine(store, millis(15_000));

        let samples = instant_vector(&engine, "up", 30_000).await;
        check!(
            (
                samples.len(),
                &samples[0].labels,
                samples[0].ts_ms,
                approx_eq(float_value(&samples[0].value), 2.0),
            ) == (
                1,
                &crate::PromqlLabels::from(labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "up"),
                    crate::test_support::TestLabelPair::new("job", "api")
                ])),
                30_000,
                true,
            )
        );
    }
}

mod instant_selector_stale_marker_terminates_series_before_lookback_expiry {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_selector_stale_marker_terminates_series_before_lookback_expiry() {
        let store = up_series_store(&[(10_000, 1.0), (20_000, stale_nan())]);
        let engine = lookback_engine(store, millis(60_000));
        let samples = instant_vector(&engine, "up", 30_000).await;
        assert2::assert!(samples.is_empty());
    }
}

mod instant_subquery_aligns_start_to_step_grid {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_subquery_aligns_start_to_step_grid() {
        let mut store = InMemoryMetricStore::new();
        let values = [
            1.0, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, 34.0, 55.0, 89.0, 144.0,
        ];
        for index in 0..values.len() {
            store.push_float(
                "tenant-a",
                labels(&[crate::test_support::TestLabelPair::new(
                    "__name__",
                    "metric_total",
                )]),
                i64::try_from(index).unwrap() * 7_000,
                values[index],
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(&engine, "rate(metric_total[1m500ms:10s])", 80_000).await;
        assert2::assert!(samples.len() == 1);
        assert2::assert!(approx_eq(
            float_value(&samples[0].value),
            2.366_666_666_666_666_7,
        ));
    }
}

mod instant_subquery_evaluates_expression_at_explicit_steps {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_subquery_evaluates_expression_at_explicit_steps() {
        let engine = queue_depth_engine();
        let result = engine
            .query_instant(&tenant_id("tenant-a"), "(queue_depth * 2)[2m:1m]", 120_000)
            .await
            .unwrap();

        let series = lone_matrix_series(&result);
        check!(series.labels.get("__name__").is_none());
        check!(series.labels.get("job") == Some("api"));
        check_series_points(
            series,
            &[
                ExpectedPoint {
                    ts_ms: 0,
                    value: 2.0,
                },
                ExpectedPoint {
                    ts_ms: 60_000,
                    value: 4.0,
                },
                ExpectedPoint {
                    ts_ms: 120_000,
                    value: 6.0,
                },
            ],
        );
    }
}

mod instant_subquery_uses_global_eval_interval_when_step_is_omitted {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_subquery_uses_global_eval_interval_when_step_is_omitted() {
        let store = queue_depth_store(&[(0, 1.0), (30_000, 2.0), (60_000, 3.0), (90_000, 4.0)]);

        let engine = PromqlEngine::new(
            Arc::new(store),
            EngineOpts {
                eval_interval: millis(30_000),
                ..EngineOpts::default()
            },
        );
        let result = engine
            .query_instant(&tenant_id("tenant-a"), "queue_depth[90s:]", 90_000)
            .await
            .unwrap();

        let QueryResult::RangeMatrix(series) = result else {
            panic!("expected matrix");
        };
        check!(series.len() == 1);
        let timestamps = series[0]
            .samples
            .iter()
            .map(|sample| sample.timestamp_ms)
            .collect::<Vec<_>>();
        check!(timestamps == [0, 30_000, 60_000, 90_000]);
    }
}
