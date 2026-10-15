use super::*;

mod empty_valued_label_planner_path_matches_interpreter {
    use super::*;

    type SamplePoint = FloatSampleInput;
    type SeriesFixture = LabeledFloatSeries;

    struct RangeQueryBounds {
        start_ms: i64,
        end_ms: i64,
    }

    /// Differential parity for a label that is present with an empty value.
    ///
    /// A series with `__unit__=""` has the label present and the value empty. That
    /// series must stay distinct from a series of the same name that has no
    /// `__unit__` label, through to the operator leaf. The operator leaf encodes an
    /// absent label as NULL and a present empty label as `""`. The planner
    /// instant-selector path and the rate-range path must produce the byte-exact
    /// result that the interpreter produces: the same series set, the same
    /// labelsets, and the same per-series values.
    #[tokio::test]
    pub(crate) async fn empty_valued_label_planner_path_matches_interpreter() {
        use promql_parser::parser::Expr;

        use crate::{DurationExprContext, parse_promql_with_duration_context};

        let mut store = InMemoryMetricStore::new();
        // Three series sharing `__name__=m`, distinguished only by the presence
        // and value of `__unit__`:
        //   - job=a: `__unit__=""`  (PRESENT, empty value)
        //   - job=b: `__unit__="s"` (PRESENT, non-empty)
        //   - job=c: `__unit__` ABSENT
        // The fingerprints of a (present-empty) and c (absent) differ, so both
        // must survive selection as distinct series.
        for fixture in [
            SeriesFixture {
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "m"),
                    crate::test_support::TestLabelPair::new("job", "a"),
                    crate::test_support::TestLabelPair::new("__unit__", ""),
                ])
                .into(),
                samples: vec![
                    SamplePoint {
                        timestamp_ms: 0,
                        value: 1.0,
                    },
                    SamplePoint {
                        timestamp_ms: 60_000,
                        value: 2.0,
                    },
                    SamplePoint {
                        timestamp_ms: 120_000,
                        value: 3.0,
                    },
                ],
            },
            SeriesFixture {
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "m"),
                    crate::test_support::TestLabelPair::new("job", "b"),
                    crate::test_support::TestLabelPair::new("__unit__", "s"),
                ])
                .into(),
                samples: vec![
                    SamplePoint {
                        timestamp_ms: 0,
                        value: 10.0,
                    },
                    SamplePoint {
                        timestamp_ms: 60_000,
                        value: 20.0,
                    },
                    SamplePoint {
                        timestamp_ms: 120_000,
                        value: 30.0,
                    },
                ],
            },
            SeriesFixture {
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "m"),
                    crate::test_support::TestLabelPair::new("job", "c"),
                ])
                .into(),
                samples: vec![
                    SamplePoint {
                        timestamp_ms: 0,
                        value: 100.0,
                    },
                    SamplePoint {
                        timestamp_ms: 60_000,
                        value: 200.0,
                    },
                    SamplePoint {
                        timestamp_ms: 120_000,
                        value: 300.0,
                    },
                ],
            },
        ] {
            push_float_series(&mut store, [fixture]);
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected instant vector");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };

        // (a) INSTANT selector path: the bare selector `m` matches all three
        // series. Planner (operator leaf) must equal the interpreter, preserving
        // the present-empty-vs-absent distinction.
        let time_ms = 120_000_i64;
        for query in ["m", "m{__unit__=\"\"}", "m{__unit__!=\"\"}"] {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
            let Expr::VectorSelector(selector) = expr else {
                panic!("`{query}` did not parse to a bare vector selector");
            };
            let interpreter = normalize(
                engine
                    .eval_instant_selector("t", &selector, time_ms)
                    .await
                    .unwrap(),
            );
            let planner = normalize(
                engine
                    .eval_instant_selector_via_planner("t", &selector, time_ms)
                    .await
                    .unwrap(),
            );
            assert2::assert!(instant_samples_match(&interpreter, &planner));
            // The plannable gate must route the empty-valued selector through the
            // operator path now (no `Ok(None)` fallback).
            let routed = engine
                .plan_instant_expr("t", &Expr::VectorSelector(selector.clone()), time_ms)
                .await
                .unwrap();
            assert2::assert!(routed.is_some());
        }

        // The bare selector `m` must yield exactly three rows (a, b, c) — proving
        // the present-empty (a) and absent (c) series were not collapsed.
        let bare = normalize(
            engine
                .query_instant(&tenant_id("t"), "m", time_ms)
                .await
                .unwrap(),
        );
        assert2::assert!(bare.len() == 3);

        // (b) RANGE/matrix path: a rate over the empty-valued-label series must
        // also route through the operator leaf and keep the present-empty (a),
        // non-empty (b), and absent (c) series DISTINCT — three separate result
        // series, the present-empty/absent pair not collapsed.
        let bounds = RangeQueryBounds {
            start_ms: 0,
            end_ms: 120_000,
        };
        let query = "rate(m[2m])";
        let QueryResult::RangeMatrix(mut series) = engine
            .query_range(
                &tenant_id("t"),
                query,
                bounds.start_ms,
                bounds.end_ms,
                millis(60_000),
            )
            .await
            .unwrap()
        else {
            panic!("expected matrix for `{query}`");
        };
        series.sort_by_key(|s| s.labels.fingerprint());
        assert2::assert!(series.len() == 3);
        // All three result series carry DISTINCT labelsets (the present-empty and
        // absent `__unit__` were not merged): distinct fingerprints.
        let fps: std::collections::BTreeSet<_> =
            series.iter().map(|s| s.labels.fingerprint()).collect();
        assert2::assert!(fps.len() == 3);
    }
}

mod experimental_call_planner_path_matches_interpreter {
    #[cfg(feature = "experimental-functions")]
    use super::*;

    #[cfg(feature = "experimental-functions")]
    struct GaugeSample {
        timestamp_ms: i64,
        value: f64,
    }

    /// Differential parity for the experimental scalar and range functions.
    ///
    /// The functions are `max_of`, `min_of`, `double_exponential_smoothing` over a
    /// bare matrix selector, and the duration helpers. Each one delegates to the
    /// same interpreter method, so the result is parity-exact by construction.
    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    pub(crate) async fn experimental_call_planner_path_matches_interpreter() {
        let mut store = InMemoryMetricStore::new();
        for sample in [
            GaugeSample {
                timestamp_ms: 0,
                value: 1.0,
            },
            GaugeSample {
                timestamp_ms: 60_000,
                value: 2.0,
            },
            GaugeSample {
                timestamp_ms: 120_000,
                value: 4.0,
            },
            GaugeSample {
                timestamp_ms: 180_000,
                value: 8.0,
            },
            GaugeSample {
                timestamp_ms: 240_000,
                value: 16.0,
            },
        ] {
            store.push_float(
                "t",
                labels(&[crate::test_support::TestLabelPair::new("__name__", "g")]),
                sample.timestamp_ms,
                sample.value,
            );
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let queries: &[QueryAt] = &[
            QueryAt {
                query: "max_of(1, 2)",
                time_ms: 120_000,
            },
            QueryAt {
                query: "min_of(1, 2)",
                time_ms: 120_000,
            },
            QueryAt {
                query: "max_of(scalar(g), 3)",
                time_ms: 240_000,
            },
            QueryAt {
                query: "double_exponential_smoothing(g[4m], 0.5, 0.5)",
                time_ms: 240_000,
            },
            // Duration helpers (instant query: no range context -> 0 on both
            // paths).
            QueryAt {
                query: "step()",
                time_ms: 120_000,
            },
            QueryAt {
                query: "start()",
                time_ms: 120_000,
            },
            QueryAt {
                query: "end()",
                time_ms: 120_000,
            },
            QueryAt {
                query: "range()",
                time_ms: 120_000,
            },
        ];

        // The recursive planner must claim every one of these.
        assert_call_planner_matches_interpreter(&engine, &queries).await;
    }
}

mod experimental_param_aggregate_planner_path_matches_interpreter {
    #[cfg(feature = "experimental-functions")]
    use super::*;

    #[cfg(feature = "experimental-functions")]
    #[derive(bon::Builder)]
    struct ParamAggregateSeries {
        instance: &'static str,
        value: f64,
    }

    /// Differential parity for the experimental `limitk` and `limit_ratio` aggregations.
    ///
    /// These are param aggregations, and the test covers the `InvalidRatioWarning`
    /// annotation of `limit_ratio`. The planner reuses the same
    /// parameter-resolution helpers and selection kernels as the interpreter, so
    /// the result and the emitted annotations both match.
    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    pub(crate) async fn experimental_param_aggregate_planner_path_matches_interpreter() {
        use crate::{DurationExprContext, parse_promql_with_duration_context};

        let mut store = InMemoryMetricStore::new();
        for series in [
            ParamAggregateSeries::builder()
                .instance("a")
                .value(1.0)
                .build(),
            ParamAggregateSeries::builder()
                .instance("b")
                .value(2.0)
                .build(),
            ParamAggregateSeries::builder()
                .instance("c")
                .value(3.0)
                .build(),
            ParamAggregateSeries::builder()
                .instance("d")
                .value(4.0)
                .build(),
        ] {
            store.push_float(
                "t",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "m"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                    crate::test_support::TestLabelPair::new("instance", series.instance),
                ]),
                120_000,
                series.value,
            );
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let queries: &[&str] = &[
            "limitk(2, m)",
            "limitk(10, m)",
            "limitk(0, m)",
            "limitk(2, m) by (job)",
            "limit_ratio(0.5, m)",
            "limit_ratio(-0.5, m)",
            "limit_ratio(1, m)",
            "limit_ratio(0, m)",
            // Out-of-range ratios: must emit the InvalidRatioWarning on BOTH paths.
            "limit_ratio(1.5, m)",
            "limit_ratio(-2, m)",
        ];
        let time_ms = 120_000_i64;

        for &query in queries {
            let parity = annotated_planned_and_interpreted(&engine, query, time_ms).await;
            let via_operators = sort_instant_result(parity.via_operators.result);
            let via_interpreter = sort_instant_result(parity.via_interpreter.result);

            assert2::assert!(query_results_match(&via_interpreter, &via_operators));
            assert2::assert!(
                parity.via_operators.annotations == parity.via_interpreter.annotations
            );
        }

        // Pin that an out-of-range ratio actually emits the warning (so the
        // equality above is not vacuously comparing two empty sets).
        let expr = parse_promql_with_duration_context(
            "limit_ratio(1.5, m)",
            DurationExprContext::instant(time_ms),
        )
        .unwrap();
        let annotations = super::super::super::ANNOTATIONS
            .scope(std::cell::RefCell::new(crate::Annotations::new()), async {
                let plan = engine
                    .plan_instant_expr("t", &expr, time_ms)
                    .await
                    .unwrap()
                    .unwrap();
                engine
                    .assemble_planned_instant(plan, time_ms)
                    .await
                    .unwrap();
                super::super::super::ANNOTATIONS.with(|sink| sink.borrow().clone())
            })
            .await;
        assert2::assert!(annotations.warnings.len() == 1);
        assert2::assert!(
            annotations.warnings[0].contains("ratio value should be between -1 and 1") == true
        );
    }
}

mod info_planner_path_matches_interpreter {
    use super::*;

    struct InfoSampleFixture {
        labels: crate::PromqlLabels,
        value: f64,
    }

    fn matching_metric_labels() -> Labels {
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "metric"),
            crate::test_support::TestLabelPair::new("instance", "a"),
            crate::test_support::TestLabelPair::new("job", "1"),
            crate::test_support::TestLabelPair::new("label", "value"),
        ])
    }

    /// Differential parity for `info(v [, data_label_selector])` in the planner.
    ///
    /// The recursive planner recurses the input vector. It selects the
    /// `target_info` series or the custom-selector series through the shared
    /// interpreter helper, then applies the shared `apply_info` join. The result
    /// must equal the interpreter's `eval_info_call` byte-for-byte.
    #[tokio::test]
    pub(crate) async fn info_planner_path_matches_interpreter() {
        use crate::{DurationExprContext, parse_promql_with_duration_context};

        // A store mirroring the conformance corpus: a base metric, a metric whose
        // identifying labels don't match any target_info, a metric with an
        // overlapping data label, plus `target_info` and `build_info` series.
        let mut store = InMemoryMetricStore::new();
        for fixture in [
            InfoSampleFixture {
                labels: matching_metric_labels().into(),
                value: 2.0,
            },
            InfoSampleFixture {
                labels: labels(&[
                    crate::test_support::TestLabelPair::new(
                        "__name__",
                        "metric_not_matching_target_info",
                    ),
                    crate::test_support::TestLabelPair::new("instance", "a"),
                    crate::test_support::TestLabelPair::new("job", "2"),
                    crate::test_support::TestLabelPair::new("label", "value"),
                ])
                .into(),
                value: 2.0,
            },
            InfoSampleFixture {
                labels: labels(&[
                    crate::test_support::TestLabelPair::new(
                        "__name__",
                        "metric_with_overlapping_label",
                    ),
                    crate::test_support::TestLabelPair::new("instance", "a"),
                    crate::test_support::TestLabelPair::new("job", "1"),
                    crate::test_support::TestLabelPair::new("label", "value"),
                    crate::test_support::TestLabelPair::new("data", "base"),
                ])
                .into(),
                value: 2.0,
            },
            InfoSampleFixture {
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "target_info"),
                    crate::test_support::TestLabelPair::new("instance", "a"),
                    crate::test_support::TestLabelPair::new("job", "1"),
                    crate::test_support::TestLabelPair::new("data", "info"),
                    crate::test_support::TestLabelPair::new("another_data", "another info"),
                ])
                .into(),
                value: 1.0,
            },
            InfoSampleFixture {
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "build_info"),
                    crate::test_support::TestLabelPair::new("instance", "a"),
                    crate::test_support::TestLabelPair::new("job", "1"),
                    crate::test_support::TestLabelPair::new("build_data", "build"),
                ])
                .into(),
                value: 1.0,
            },
        ] {
            store.push_float("t", fixture.labels, 600_000, fixture.value);
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        // Each query must route through the operator path and match the
        // interpreter exactly: default target_info enrichment, single/all-label
        // restriction, non-matching identifying labels (passthrough), a required
        // matcher not matching empty (drop), overlapping-label passthrough, and
        // explicit `__name__` selectors (target_info / build_info / both /
        // non-existent), plus the input-as-bare-selector form.
        let queries = [
            "info(metric)",
            r#"info(metric, {data=~".+"})"#,
            "info(metric_not_matching_target_info)",
            r#"info(metric, {non_existent=~".+"})"#,
            r#"info(metric, {data=~".+", non_existent=~".*"})"#,
            "info(metric_with_overlapping_label)",
            r#"info(metric, {__name__="target_info"})"#,
            r#"info(metric, {__name__="non_existent"})"#,
            r#"info(metric, {__name__="build_info"})"#,
            r#"info(metric, {__name__=~".+_info"})"#,
            r#"info(build_info, {__name__=~".+_info", build_data=~".+"})"#,
            // Input as a bare brace-only selector.
            r#"info({job="1"}, {__name__="target_info"})"#,
        ];

        for query in queries {
            let parity = planned_and_interpreted(&engine, query, 600_000).await;
            let via_operators = parity.via_operators;
            let via_interpreter = parity.via_interpreter;

            let normalize = |result: QueryResult| fingerprint_sorted(result, query);

            let interpreter = normalize(via_interpreter);
            let operators = normalize(via_operators);
            assert2::assert!(instant_samples_match(&interpreter, &operators));
        }

        // A histogram info-series match errors identically (the info series must be
        // float-typed). Pin that the planner surfaces the same error class.
        let mut hist_store = InMemoryMetricStore::new();
        hist_store.push_float("t", matching_metric_labels(), 600_000, 2.0);
        hist_store.push_histogram(
            "t",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "hist"),
                crate::test_support::TestLabelPair::new("instance", "a"),
                crate::test_support::TestLabelPair::new("job", "1"),
            ]),
            600_000,
            native_histogram(4.0, 10.0),
        );
        let hist_engine = PromqlEngine::new(Arc::new(hist_store), EngineOpts::default());
        let hist_query = r#"info(metric, {__name__="hist"})"#;
        let hist_expr =
            parse_promql_with_duration_context(hist_query, DurationExprContext::instant(600_000))
                .unwrap();
        let operator_result = hist_engine
            .plan_instant_expr("t", &hist_expr, 600_000)
            .await;
        assert2::assert!(matches!(operator_result, Err(PromqlError::Exec(_))));
    }
}

mod label_ops_planner_path_matches_interpreter {
    use super::*;

    #[derive(bon::Builder)]
    struct LabelOpsFloatSample {
        labels: Labels,
        value: f64,
    }

    struct LabelOpsQueryCase {
        query: &'static str,
        time_ms: i64,
    }

    #[tokio::test]
    pub(crate) async fn label_ops_planner_path_matches_interpreter() {
        use crate::{DurationExprContext, parse_promql_with_duration_context};

        // A float-only store: a multi-label gauge (with a `src` label for
        // capture-group expansion), a genuine-NaN series (must survive the
        // operator path and sort last), and an up-like metric for the nested
        // aggregate case.
        let mut store = InMemoryMetricStore::new();
        for sample in [
            LabelOpsFloatSample::builder()
                .labels(labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "g"),
                    crate::test_support::TestLabelPair::new("l", "x"),
                    crate::test_support::TestLabelPair::new("src", "a-1"),
                ]))
                .value(3.0)
                .build(),
            LabelOpsFloatSample::builder()
                .labels(labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "g"),
                    crate::test_support::TestLabelPair::new("l", "y"),
                    crate::test_support::TestLabelPair::new("src", "b-2"),
                ]))
                .value(1.0)
                .build(),
            LabelOpsFloatSample::builder()
                .labels(labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "g"),
                    crate::test_support::TestLabelPair::new("l", "z"),
                    crate::test_support::TestLabelPair::new("src", "c-3"),
                ]))
                .value(f64::NAN)
                .build(),
        ] {
            store.push_float("t", sample.labels, 60_000, sample.value);
        }
        for job in ["api", "db"] {
            store.push_float(
                "t",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "up"),
                    crate::test_support::TestLabelPair::new("job", job),
                ]),
                60_000,
                1.0,
            );
        }
        // Two `h` series differing only in label `a`; overwriting `a` to a
        // constant collapses them onto the same labelset (the collision case).
        for sample in [
            LabelOpsFloatSample::builder()
                .labels(labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "h"),
                    crate::test_support::TestLabelPair::new("a", "1"),
                    crate::test_support::TestLabelPair::new("b", "p"),
                ]))
                .value(10.0)
                .build(),
            LabelOpsFloatSample::builder()
                .labels(labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "h"),
                    crate::test_support::TestLabelPair::new("a", "2"),
                    crate::test_support::TestLabelPair::new("b", "p"),
                ]))
                .value(20.0)
                .build(),
        ] {
            store.push_float("t", sample.labels, 60_000, sample.value);
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        // Representative label-ops queries over a plannable inner vector. Each
        // must route through the operator path and match the interpreter exactly,
        // covering: capture-group `$1` expansion, no-match passthrough,
        // delete-via-empty-replacement, multi-source label_join + separator,
        // sort/sort_desc (incl. the genuine-NaN row), and a nested aggregate.
        let queries = [
            // Capture group: `src="a-1"` -> `dst="a"`.
            LabelOpsQueryCase {
                query: r#"label_replace(g, "dst", "$1", "src", "(.*)-.*")"#,
                time_ms: 60_000,
            },
            // No match (`src` has no digit-only-prefix form here): unchanged.
            LabelOpsQueryCase {
                query: r#"label_replace(g, "dst", "$1", "src", "(\\d+)")"#,
                time_ms: 60_000,
            },
            // Empty replacement writes `dst=""` (the interpreter keeps it).
            LabelOpsQueryCase {
                query: r#"label_replace(g, "dst", "", "src", ".*")"#,
                time_ms: 60_000,
            },
            // Replace the metric name itself (label_replace does not drop it).
            LabelOpsQueryCase {
                query: r#"label_replace(g, "__name__", "renamed", "l", "(.+)")"#,
                time_ms: 60_000,
            },
            // label_join: multi-source with a separator.
            LabelOpsQueryCase {
                query: r#"label_join(g, "dst", "/", "l", "src")"#,
                time_ms: 60_000,
            },
            // label_join with a single source and empty separator.
            LabelOpsQueryCase {
                query: r#"label_join(g, "dst", "", "l")"#,
                time_ms: 60_000,
            },
            // sort / sort_desc over a bare selector, including the NaN row
            // (which the NaN-preserving inner sourcing must keep and place last).
            LabelOpsQueryCase {
                query: "sort(g)",
                time_ms: 60_000,
            },
            LabelOpsQueryCase {
                query: "sort_desc(g)",
                time_ms: 60_000,
            },
            // Nested compositional case: sort over an aggregate (NaN-free `up`,
            // so the aggregate operator path matches the interpreter exactly).
            LabelOpsQueryCase {
                query: "sort(sum by (job) (up))",
                time_ms: 60_000,
            },
            // label_replace over a nested aggregate (operator inner).
            LabelOpsQueryCase {
                query: r#"label_replace(sum by (job) (up), "tag", "$1", "job", "(.+)")"#,
                time_ms: 60_000,
            },
            // Binary operands are now planner-supported, so label-ops over a
            // binary inner expression route through operators and must match the
            // interpreter (note: `g + 1` drops `__name__`, so `l`/`src` survive).
            LabelOpsQueryCase {
                query: r#"label_join(g + 1, "dst", "/", "l")"#,
                time_ms: 60_000,
            },
            LabelOpsQueryCase {
                query: "sort(g + 1)",
                time_ms: 60_000,
            },
            // sort_by_label / sort_by_label_desc over a bare selector: order by the
            // `l` label values, then by remaining labels (the canonical key
            // tiebreak). Order-sensitive (the comparator below treats `sort*`
            // queries as ordered).
            LabelOpsQueryCase {
                query: r#"sort_by_label(g, "l")"#,
                time_ms: 60_000,
            },
            LabelOpsQueryCase {
                query: r#"sort_by_label_desc(g, "l")"#,
                time_ms: 60_000,
            },
            // Multi-label sort_by_label: tie on `l` would fall through to `src`.
            LabelOpsQueryCase {
                query: r#"sort_by_label(g, "l", "src")"#,
                time_ms: 60_000,
            },
            // sort_by_label over a nested aggregate (operator inner).
            LabelOpsQueryCase {
                query: r#"sort_by_label(sum by (job) (up), "job")"#,
                time_ms: 60_000,
            },
        ];

        for case in queries {
            let query = case.query;
            let parity = planned_and_interpreted(&engine, query, case.time_ms).await;
            let via_operators = parity.via_operators;
            let via_interpreter = parity.via_interpreter;

            // `sort`/`sort_desc` assert ordering, so compare order-sensitively for
            // them and fingerprint-normalize the unordered label-rewrites.
            let ordered = query.starts_with("sort");
            let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
                let QueryResult::InstantVector(mut samples) = result else {
                    panic!("expected vector for `{query}`");
                };
                if !ordered {
                    samples.sort_by(|left, right| {
                        left.labels.fingerprint().cmp(&right.labels.fingerprint())
                    });
                }
                samples
            };

            let interpreter = normalize(via_interpreter);
            let operators = normalize(via_operators);
            assert2::assert!(nan_equal_samples(&interpreter, &operators));
        }

        // A `label_replace` that collapses two series onto the same labelset must
        // error identically through the operator path and the interpreter. The
        // top-level uniqueness check enforces this for both (`query_instant`).
        let collision = r#"label_replace(h, "a", "same", "a", ".*")"#;
        let operator_err = engine
            .query_instant(&tenant_id("t"), collision, 60_000)
            .await
            .expect_err("collision must error through the operator path");
        assert2::assert!(matches!(operator_err, PromqlError::Exec(_)));
        // Confirm the operator path actually claimed the collision query (so the
        // error came from the operator path, not an interpreter fallback).
        let collision_expr =
            parse_promql_with_duration_context(collision, DurationExprContext::instant(60_000))
                .unwrap();
        assert2::assert!(
            engine
                .plan_instant_expr("t", &collision_expr, 60_000)
                .await
                .unwrap()
                .is_some()
        );

        // `sort_by_label` / `sort_by_label_desc` now route through the operator
        // path (differentially checked in the `queries` list above); pin that the
        // planner claims them, including calls with no label-name argument.
        for query in [r#"sort_by_label(g, "l")"#, r#"sort_by_label_desc(g, "l")"#] {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(60_000))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
            let planned = engine
                .plan_instant_expr("t", &expr, 60_000)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}`: {error}"));
            assert2::assert!(planned.is_some());
        }
        // An omitted label list sorts by the full labels as the tie breaker.
        let no_label = parse_promql_with_duration_context(
            "sort_by_label(g)",
            DurationExprContext::instant(60_000),
        )
        .unwrap();
        assert2::assert!(
            engine
                .plan_instant_expr("t", &no_label, 60_000)
                .await
                .unwrap()
                .is_some()
        );
    }
}

mod scalar_math_planner_path_matches_interpreter {
    use super::*;

    #[derive(bon::Builder)]
    struct ScalarMathFloatSample {
        labels: Labels,
        timestamp_ms: i64,
        value: f64,
    }

    struct ScalarMathQueryCase {
        query: &'static str,
        time_ms: i64,
    }

    #[tokio::test]
    pub(crate) async fn scalar_math_planner_path_matches_interpreter() {
        // A float-only store: a multi-label gauge with negatives (for
        // `sqrt`/`ln` NaN/-inf edges), a genuine-NaN series (must survive the
        // operator path), an up-like series for the nested aggregate case, and a
        // counter for the nested rate case.
        let mut store = InMemoryMetricStore::new();
        for sample in [
            ScalarMathFloatSample::builder()
                .labels(labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "g"),
                    crate::test_support::TestLabelPair::new("l", "x"),
                ]))
                .timestamp_ms(60_000)
                .value(-3.0)
                .build(),
            ScalarMathFloatSample::builder()
                .labels(labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "g"),
                    crate::test_support::TestLabelPair::new("l", "y"),
                ]))
                .timestamp_ms(60_000)
                .value(20.0)
                .build(),
            ScalarMathFloatSample::builder()
                .labels(labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "g"),
                    crate::test_support::TestLabelPair::new("l", "z"),
                ]))
                .timestamp_ms(60_000)
                .value(f64::NAN)
                .build(),
            ScalarMathFloatSample::builder()
                .labels(labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "up"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                ]))
                .timestamp_ms(60_000)
                .value(1.0)
                .build(),
            ScalarMathFloatSample::builder()
                .labels(labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "up"),
                    crate::test_support::TestLabelPair::new("job", "db"),
                ]))
                .timestamp_ms(60_000)
                .value(1.0)
                .build(),
        ] {
            store.push_float("t", sample.labels, sample.timestamp_ms, sample.value);
        }
        // A counter with a few samples for `abs(rate(...))`.
        let ctr = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "c"),
            crate::test_support::TestLabelPair::new("job", "api"),
        ]);
        for sample in [
            ScalarMathFloatSample::builder()
                .labels(ctr.clone())
                .timestamp_ms(0)
                .value(0.0)
                .build(),
            ScalarMathFloatSample::builder()
                .labels(ctr.clone())
                .timestamp_ms(60_000)
                .value(30.0)
                .build(),
            ScalarMathFloatSample::builder()
                .labels(ctr.clone())
                .timestamp_ms(120_000)
                .value(90.0)
                .build(),
        ] {
            store.push_float("t", sample.labels, sample.timestamp_ms, sample.value);
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        // Representative scalar-math queries over a plannable inner vector. Each
        // must route through the operator path and match the interpreter exactly,
        // including the genuine-NaN row and `sqrt(neg)`/`ln(neg)` -> NaN.
        let queries = [
            ScalarMathQueryCase {
                query: "abs(g)",
                time_ms: 60_000,
            },
            ScalarMathQueryCase {
                query: "sqrt(g)",
                time_ms: 60_000,
            },
            ScalarMathQueryCase {
                query: "ln(g)",
                time_ms: 60_000,
            },
            ScalarMathQueryCase {
                query: "log2(g)",
                time_ms: 60_000,
            },
            ScalarMathQueryCase {
                query: "sgn(g)",
                time_ms: 60_000,
            },
            ScalarMathQueryCase {
                query: "ceil(g)",
                time_ms: 60_000,
            },
            ScalarMathQueryCase {
                query: "floor(g)",
                time_ms: 60_000,
            },
            ScalarMathQueryCase {
                query: "exp(g)",
                time_ms: 60_000,
            },
            ScalarMathQueryCase {
                query: "sin(g)",
                time_ms: 60_000,
            },
            ScalarMathQueryCase {
                query: "cos(g)",
                time_ms: 60_000,
            },
            ScalarMathQueryCase {
                query: "atan(g)",
                time_ms: 60_000,
            },
            ScalarMathQueryCase {
                query: "deg(g)",
                time_ms: 60_000,
            },
            ScalarMathQueryCase {
                query: "rad(g)",
                time_ms: 60_000,
            },
            ScalarMathQueryCase {
                query: "round(g)",
                time_ms: 60_000,
            },
            ScalarMathQueryCase {
                query: "round(g, 5)",
                time_ms: 60_000,
            },
            ScalarMathQueryCase {
                query: "clamp_min(g, 0)",
                time_ms: 60_000,
            },
            ScalarMathQueryCase {
                query: "clamp_max(g, 10)",
                time_ms: 60_000,
            },
            ScalarMathQueryCase {
                query: "clamp(g, 0, 10)",
                time_ms: 60_000,
            },
            // `min > max` yields the empty vector.
            ScalarMathQueryCase {
                query: "clamp(g, 10, 0)",
                time_ms: 60_000,
            },
            // Nested compositional cases: scalar math over rate and over an
            // aggregate, both already on the operator path.
            ScalarMathQueryCase {
                query: "abs(rate(c[5m]))",
                time_ms: 120_000,
            },
            ScalarMathQueryCase {
                query: "ceil(sum by (job) (up))",
                time_ms: 60_000,
            },
            // Binary operands are now planner-supported, so scalar math over a
            // binary inner expression also routes through operators and must
            // match the interpreter (incl. the genuine-NaN row in `g`).
            ScalarMathQueryCase {
                query: "abs(g + 1)",
                time_ms: 60_000,
            },
            // `atan2` is a binary operator returning a vector; it routes through
            // the binary planner path and must match the interpreter.
            ScalarMathQueryCase {
                query: "g atan2 g",
                time_ms: 60_000,
            },
        ];

        for case in queries {
            let SortedParity {
                via_operators: operators,
                via_interpreter: interpreter,
            } = sorted_planned_and_interpreted(&engine, case.query, case.time_ms).await;
            assert2::assert!(nan_equal_samples(&interpreter, &operators));
        }

        // A bare matrix selector now routes through the planner as a
        // `RangeMatrix` result (covered by `matrix_subquery_planner_path_matches_
        // interpreter`), so it is no longer asserted as a scalar-math fall-back
        // here.
    }
}

mod structural_node_planner_path_matches_interpreter {
    use super::*;

    #[derive(bon::Builder)]
    struct FloatSampleCase {
        labels: krabka_blockstore::Labels,
        timestamp_ms: i64,
        value: f64,
    }

    impl FloatSampleSeed for FloatSampleCase {
        fn push_into(self, store: &mut InMemoryMetricStore) {
            store.push_float("t", self.labels, self.timestamp_ms, self.value);
        }
    }

    struct QueryResultTypeCase {
        query: &'static str,
        time_ms: i64,
        expected_type: &'static str,
    }

    /// Differential parity for the top-level structural node kinds.
    ///
    /// The node kinds are unary negation, a bare numeric literal, a bare string
    /// literal, a raw matrix selector, a subquery, and the `smoothed` extended
    /// selector. `query_instant` returns a `RangeMatrix` for the raw matrix
    /// selector and for the subquery. Through the operator planner, each kind must
    /// produce the byte-exact result that the interpreter's `eval_instant_expr`
    /// produces.
    #[tokio::test]
    pub(crate) async fn structural_node_planner_path_matches_interpreter() {
        use crate::{DurationExprContext, parse_promql_with_duration_context};

        let mut store = InMemoryMetricStore::new();
        let stale_bits = stale_nan();
        push_float_samples(
            &mut store,
            [
                FloatSampleCase::builder()
                    .labels(labels(&[
                        crate::test_support::TestLabelPair::new("__name__", "m"),
                        crate::test_support::TestLabelPair::new("job", "api"),
                    ]))
                    .timestamp_ms(60_000)
                    .value(2.0)
                    .build(),
                FloatSampleCase::builder()
                    .labels(labels(&[
                        crate::test_support::TestLabelPair::new("__name__", "m"),
                        crate::test_support::TestLabelPair::new("job", "api"),
                    ]))
                    .timestamp_ms(120_000)
                    .value(3.0)
                    .build(),
                FloatSampleCase::builder()
                    .labels(labels(&[
                        crate::test_support::TestLabelPair::new("__name__", "m"),
                        crate::test_support::TestLabelPair::new("job", "db"),
                    ]))
                    .timestamp_ms(120_000)
                    .value(7.0)
                    .build(),
                // A genuine NaN latest-in-window sample (kept, negated to NaN).
                FloatSampleCase::builder()
                    .labels(labels(&[
                        crate::test_support::TestLabelPair::new("__name__", "m"),
                        crate::test_support::TestLabelPair::new("job", "nan"),
                    ]))
                    .timestamp_ms(120_000)
                    .value(f64::NAN)
                    .build(),
                // A stale marker (dropped on both paths).
                FloatSampleCase::builder()
                    .labels(labels(&[
                        crate::test_support::TestLabelPair::new("__name__", "m"),
                        crate::test_support::TestLabelPair::new("job", "stale"),
                    ]))
                    .timestamp_ms(120_000)
                    .value(stale_bits)
                    .build(),
                // A series with a short history for the matrix / subquery / smoothed shapes.
                FloatSampleCase::builder()
                    .labels(labels(&[crate::test_support::TestLabelPair::new(
                        "__name__", "g",
                    )]))
                    .timestamp_ms(0)
                    .value(1.0)
                    .build(),
                FloatSampleCase::builder()
                    .labels(labels(&[crate::test_support::TestLabelPair::new(
                        "__name__", "g",
                    )]))
                    .timestamp_ms(60_000)
                    .value(2.0)
                    .build(),
                FloatSampleCase::builder()
                    .labels(labels(&[crate::test_support::TestLabelPair::new(
                        "__name__", "g",
                    )]))
                    .timestamp_ms(120_000)
                    .value(4.0)
                    .build(),
                FloatSampleCase::builder()
                    .labels(labels(&[crate::test_support::TestLabelPair::new(
                        "__name__", "g",
                    )]))
                    .timestamp_ms(180_000)
                    .value(8.0)
                    .build(),
                FloatSampleCase::builder()
                    .labels(labels(&[crate::test_support::TestLabelPair::new(
                        "__name__", "g",
                    )]))
                    .timestamp_ms(240_000)
                    .value(16.0)
                    .build(),
            ],
        );
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let queries: &[QueryAt] = &[
            // Unary over a vector (drops `__name__`, negates each value, keeps a
            // genuine NaN, drops the stale marker).
            QueryAt {
                query: "-m",
                time_ms: 120_000,
            },
            // Unary over an aggregate result (vector).
            QueryAt {
                query: "-sum(m)",
                time_ms: 120_000,
            },
            // Unary over a scalar.
            QueryAt {
                query: "-(1 + 2)",
                time_ms: 120_000,
            },
            // Double negation.
            QueryAt {
                query: "- -m",
                time_ms: 120_000,
            },
            // Bare numeric / string literals.
            QueryAt {
                query: "42",
                time_ms: 120_000,
            },
            QueryAt {
                query: "-7.5",
                time_ms: 120_000,
            },
            QueryAt {
                query: r#""hello""#,
                time_ms: 120_000,
            },
            // Raw matrix selector / subquery (RangeMatrix from query_instant).
            QueryAt {
                query: "g[3m]",
                time_ms: 240_000,
            },
            QueryAt {
                query: "m[2m]",
                time_ms: 120_000,
            },
            QueryAt {
                query: "g[4m:1m]",
                time_ms: 240_000,
            },
            // `smoothed` extended selector (vector). The extension parser is not
            // feature-gated, so this routes in both build configs.
            QueryAt {
                query: "smoothed(g)",
                time_ms: 90_000,
            },
        ];

        // The recursive planner must claim every one of these.
        assert_call_planner_matches_interpreter(&engine, &queries).await;

        // Pin the result types the parity above relies on.
        for case in [
            QueryResultTypeCase {
                query: "42",
                time_ms: 120_000,
                expected_type: "scalar",
            },
            QueryResultTypeCase {
                query: r#""hello""#,
                time_ms: 120_000,
                expected_type: "string",
            },
            QueryResultTypeCase {
                query: "g[3m]",
                time_ms: 240_000,
                expected_type: "matrix",
            },
            QueryResultTypeCase {
                query: "g[4m:1m]",
                time_ms: 240_000,
                expected_type: "matrix",
            },
            QueryResultTypeCase {
                query: "-m",
                time_ms: 120_000,
                expected_type: "vector",
            },
            QueryResultTypeCase {
                query: "-(1 + 2)",
                time_ms: 120_000,
                expected_type: "scalar",
            },
        ] {
            let expr = parse_promql_with_duration_context(
                case.query,
                DurationExprContext::instant(case.time_ms),
            )
            .unwrap();
            let plan = engine
                .plan_instant_expr("t", &expr, case.time_ms)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("`{}` did not route through the planner", case.query));
            let result = engine
                .assemble_planned_instant(plan, case.time_ms)
                .await
                .unwrap();
            assert2::assert!(result.result_type() == case.expected_type);
        }

        // The `anchored` modifier on an instant-vector selector is the same hard
        // error on both paths.
        {
            let expr = parse_promql_with_duration_context(
                "anchored(m)",
                DurationExprContext::instant(120_000),
            )
            .unwrap();
            let planner_err = engine.plan_instant_expr("t", &expr, 120_000).await;
            let interp_err = engine.eval_instant_expr("t", &expr, 120_000).await;
            assert2::assert!(planner_err.is_err());
            assert2::assert!(interp_err.is_err());
        }
    }
}

mod util_planner_path_matches_interpreter {
    use super::*;

    #[derive(bon::Builder)]
    struct UtilitySampleCase {
        name: &'static str,
        job: &'static str,
        timestamp_ms: i64,
        value: f64,
    }

    struct TimestampExpectation {
        job: &'static str,
        seconds: f64,
    }

    #[tokio::test]
    pub(crate) async fn util_planner_path_matches_interpreter() {
        // NaN-aware whole-result comparison covering both scalar and vector
        // results, sorting vector samples by fingerprint first.
        fn results_match(left: QueryResult, right: QueryResult) -> bool {
            match left {
                QueryResult::Scalar {
                    ts_ms: left_ts,
                    value: left_value,
                } => match right {
                    QueryResult::Scalar {
                        ts_ms: right_ts,
                        value: right_value,
                    } => {
                        left_ts == right_ts
                            && (left_value.to_bits() == right_value.to_bits()
                                || (left_value.is_nan() && right_value.is_nan()))
                    }
                    _ => false,
                },
                QueryResult::InstantVector(mut left_samples) => match right {
                    QueryResult::InstantVector(mut right_samples) => {
                        left_samples.sort_by_key(|sample| sample.labels.fingerprint());
                        right_samples.sort_by_key(|sample| sample.labels.fingerprint());
                        nan_equal_samples(&left_samples, &right_samples)
                    }
                    _ => false,
                },
                _ => false,
            }
        }

        // A float-only store. `m{job}` carries distinct timestamps per series so
        // `timestamp(m)` differs per row; a single-series metric `solo` exercises
        // `scalar(single)`; `dup` (two series) exercises `scalar(multi)->NaN`. A
        // genuine-NaN row survives `timestamp`/calendar drops. `present` exists,
        // `gone` does not (for absent / absent_over_time).
        let mut store = InMemoryMetricStore::new();
        for case in [
            // Two `m` series at different timestamps within the lookback window.
            UtilitySampleCase::builder()
                .name("m")
                .job("api")
                .timestamp_ms(30_000)
                .value(100.0)
                .build(),
            UtilitySampleCase::builder()
                .name("m")
                .job("db")
                .timestamp_ms(60_000)
                .value(1_700_000_000.0)
                .build(),
            // A genuine-NaN `m` row (must survive timestamp/calendar, value-> ts).
            UtilitySampleCase::builder()
                .name("m")
                .job("nan")
                .timestamp_ms(45_000)
                .value(f64::NAN)
                .build(),
            // A single-series metric for scalar(single).
            UtilitySampleCase::builder()
                .name("solo")
                .job("x")
                .timestamp_ms(60_000)
                .value(42.5)
                .build(),
            // Two series sharing a name for scalar(multi)->NaN.
            UtilitySampleCase::builder()
                .name("dup")
                .job("a")
                .timestamp_ms(60_000)
                .value(1.0)
                .build(),
            UtilitySampleCase::builder()
                .name("dup")
                .job("b")
                .timestamp_ms(60_000)
                .value(2.0)
                .build(),
            // A present series for absent(present)->empty / absent_over_time.
            UtilitySampleCase::builder()
                .name("present")
                .job("p")
                .timestamp_ms(55_000)
                .value(7.0)
                .build(),
        ] {
            store.push_float(
                "t",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", case.name),
                    crate::test_support::TestLabelPair::new("job", case.job),
                ]),
                case.timestamp_ms,
                case.value,
            );
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let time_ms = 60_000_i64;

        // Each query must route through the operator path AND match the
        // interpreter (NaN-aware), covering both vector and scalar results.
        let queries = [
            // Vector-returning utilities over a plannable inner.
            "timestamp(m)",
            "timestamp(solo)",
            "day_of_week(m)",
            "day_of_month(m)",
            "day_of_year(m)",
            "days_in_month(m)",
            "hour(m)",
            "minute(m)",
            "month(m)",
            "year(m)",
            // vector(scalar) yields a single no-label series.
            "vector(42)",
            "vector(time())",
            // absent / absent_over_time, present and missing.
            "absent(present)",
            "absent(gone)",
            "absent(gone{job=\"z\"})",
            "absent_over_time(present[5m])",
            "absent_over_time(gone[5m])",
            "absent_over_time(gone{job=\"z\"}[5m])",
            // Scalar-returning utilities.
            "time()",
            "pi()",
            "scalar(solo)",
            "scalar(dup)",
            // Argless calendar forms operate on time().
            "hour()",
            "year()",
            // scalar∘scalar arithmetic folds to a scalar.
            "2 + 3 * 4",
            // calendar over a scalar-arg utility (vector arg).
            "timestamp(vector(1700000000))",
        ];

        for query in queries {
            let parity = planned_and_interpreted(&engine, query, time_ms).await;
            let via_operators = parity.via_operators;
            let via_interpreter = parity.via_interpreter;

            assert2::assert!(results_match(
                via_interpreter.clone(),
                via_operators.clone()
            ));
        }

        // Pin specific behaviors the parity above relies on.
        // 1. scalar(single) returns the lone value; scalar(multi) returns NaN.
        let QueryResult::Scalar { value: single, .. } = engine
            .query_instant(&tenant_id("t"), "scalar(solo)", time_ms)
            .await
            .unwrap()
        else {
            panic!("expected scalar");
        };
        assert2::assert!(single.to_bits() == 42.5_f64.to_bits());
        let QueryResult::Scalar { value: multi, .. } = engine
            .query_instant(&tenant_id("t"), "scalar(dup)", time_ms)
            .await
            .unwrap()
        else {
            panic!("expected scalar");
        };
        assert2::assert!(multi.is_nan());

        // 2. time() and pi() are the eval-time seconds and π.
        let QueryResult::Scalar {
            ts_ms: returned_ts,
            value: eval_seconds,
        } = engine
            .query_instant(&tenant_id("t"), "time()", time_ms)
            .await
            .unwrap()
        else {
            panic!("expected scalar");
        };
        assert2::assert!(returned_ts == time_ms);
        assert2::assert!(eval_seconds.to_bits() == 60.0_f64.to_bits());
        let QueryResult::Scalar { value: pi_v, .. } = engine
            .query_instant(&tenant_id("t"), "pi()", time_ms)
            .await
            .unwrap()
        else {
            panic!("expected scalar");
        };
        assert2::assert!(pi_v.to_bits() == std::f64::consts::PI.to_bits());

        // 3. absent(present) is empty; absent(gone{job="z"}) carries the matcher
        //    label and value 1.
        let QueryResult::InstantVector(present) = engine
            .query_instant(&tenant_id("t"), "absent(present)", time_ms)
            .await
            .unwrap()
        else {
            panic!("expected vector");
        };
        assert2::assert!(present.is_empty());
        let QueryResult::InstantVector(gone) = engine
            .query_instant(&tenant_id("t"), "absent(gone{job=\"z\"})", time_ms)
            .await
            .unwrap()
        else {
            panic!("expected vector");
        };
        assert2::assert!(gone.len() == 1);
        assert2::assert!(gone[0].labels.get("job") == Some("z"));
        assert2::assert!(gone[0].labels.get("__name__") == None);
        assert2::assert!(float_value(&gone[0].value).to_bits() == 1.0_f64.to_bits());

        // 4. timestamp(m) reports each sample's own timestamp in seconds, not the
        //    eval time, and drops __name__.
        let QueryResult::InstantVector(ts_samples) = engine
            .query_instant(&tenant_id("t"), "timestamp(m)", time_ms)
            .await
            .unwrap()
        else {
            panic!("expected vector");
        };
        assert2::assert!(
            ts_samples
                .iter()
                .all(|s| s.labels.get("__name__").is_none())
        );
        let mut by_job = std::collections::BTreeMap::new();
        for sample in &ts_samples {
            by_job.insert(
                sample.labels.get("job").unwrap(),
                float_value(&sample.value),
            );
        }
        for expected in [
            TimestampExpectation {
                job: "api",
                seconds: 30.0_f64,
            },
            TimestampExpectation {
                job: "db",
                seconds: 60.0,
            },
            TimestampExpectation {
                job: "nan",
                seconds: 45.0,
            },
        ] {
            check!(
                by_job[expected.job].to_bits() == expected.seconds.to_bits(),
                "timestamp {} row",
                expected.job
            );
        }
    }
}

mod clean_query_raises_no_annotations {
    use super::*;

    #[tokio::test]
    pub(crate) async fn clean_query_raises_no_annotations() {
        let engine = PromqlEngine::new(Arc::new(set_op_store()), EngineOpts::default());
        let annotated = engine
            .query_instant_with_annotations(&tenant_id("tenant-a"), "up", 10_000)
            .await
            .expect("query");
        assert2::assert!(annotated.annotations.is_empty());
    }
}
