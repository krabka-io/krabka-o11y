use super::*;

mod abusive_subquery_resolution_errors_before_looping {
    use super::*;

    /// C2 engine backstop: an abusive subquery resolution returns an error.
    ///
    /// The range driver's `check_resolution_points` guard returns the error. The
    /// driver does not loop about 1e11 times.
    #[tokio::test]
    pub(crate) async fn abusive_subquery_resolution_errors_before_looping() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "t",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "up")]),
            0,
            1.0,
        );
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        // `last_over_time(up[1000d:1ms])` would walk ~8.6e10 sub-steps; the
        // backstop rejects it with the resolution error instead.
        let err = engine
            .query_instant(&tenant_id("t"), "last_over_time(up[1000d:1ms])", 0)
            .await
            .expect_err("abusive subquery resolution must error");
        assert2::assert!(err.to_string().contains("exceeded maximum resolution"));
    }
}

mod check_resolution_points_enforces_cap {
    use super::*;

    /// C2: `check_resolution_points` guards the step and the point count.
    ///
    /// The function rejects a non-positive step. It rejects an abusive point count
    /// above `MAX_RESOLUTION_POINTS`, and it accepts a count at the cap.
    #[test]
    pub(crate) fn check_resolution_points_enforces_cap() {
        // A non-positive step is rejected outright.
        assert2::assert!(check_resolution_points(0, 1_000, Time::ZERO).is_err());
        assert2::assert!(check_resolution_points(0, 1_000, millis(1) * -1.0).is_err());

        // `(end-start)/step == MAX_RESOLUTION_POINTS` intervals is accepted — the
        // same boundary the HTTP gate and Prometheus' `(end-start)/step > 11000`
        // rule admit (no off-by-one re-rejection of a gate-admitted query).
        let at_cap = i64::try_from(MAX_RESOLUTION_POINTS).unwrap(); // step = 1ms => intervals == MAX.
        assert2::assert!(check_resolution_points(0, at_cap, millis(1)).is_ok());

        // One interval over the cap errors.
        assert2::assert!(check_resolution_points(0, at_cap + 1, millis(1)).is_err());

        // The abusive `[1000d:1ms]`-style resolution is rejected before looping.
        let thousand_days_ms = 1_000_i64 * 24 * 60 * 60 * 1_000;
        let err = check_resolution_points(0, thousand_days_ms, millis(1)).expect_err("must reject");
        assert2::assert!(err.to_string().contains("exceeded maximum resolution"));
    }
}

mod conformance_corpus_runs_green_through_planner {
    use super::*;
    /// Corpus green-through-the-public-entry-points guard.
    ///
    /// This test runs the full conformance corpus through the public
    /// `query_instant` and `query_range` entry points, the same way the conformance
    /// harness does, and asserts that every file passes. The tree-walking
    /// interpreter is deleted, so the operator planner is the only evaluation
    /// engine reached from these entry points. A green corpus here is then a green
    /// corpus through the planner. The direct totality proof lives in
    /// [`plan_instant_expr_is_total_over_construct_sweep`]: every valid query plans
    /// to `Ok(Some)`, every invalid one plans to `Err`, and none plans to
    /// `Ok(None)`.
    #[tokio::test]
    pub(crate) async fn conformance_corpus_runs_green_through_planner() {
        use crate::conformance::testkit::{corpus_dir, run_corpus_dir};

        let report = run_corpus_dir(corpus_dir()).await;
        // Sanity: the corpus actually ran (no path/setup error swallowed the run).
        assert2::assert!(!report.files.is_empty());
        for file in &report.files {
            assert2::assert!(file.passed, "{}: {:?}", file.name, file.error);
        }
    }
}

mod plan_instant_expr_is_total_over_construct_sweep {
    use super::*;

    type FixtureSample = FloatSampleInput;
    type FixtureSeries = LabeledFloatSeries;

    /// Direct totality assertion over a representative construct sweep.
    ///
    /// For every valid query family that the corpus can produce, `plan_instant_expr`
    /// must return `Ok(Some(..))` and route through the planner. It must never
    /// return `Ok(None)`. For every invalid query, it must return `Err(..)`, and it
    /// must never return `Ok(None)`. This test is the per-construct complement to
    /// the corpus-wide counter proof.
    #[tokio::test]
    pub(crate) async fn plan_instant_expr_is_total_over_construct_sweep() {
        use crate::{DurationExprContext, parse_promql_with_duration_context};

        let mut store = InMemoryMetricStore::new();
        let time_ms = 300_000_i64;
        for fixture in [
            FixtureSeries {
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "m"),
                    crate::test_support::TestLabelPair::new("job", "a"),
                ])
                .into(),
                samples: vec![
                    FixtureSample {
                        timestamp_ms: 120_000,
                        value: 1.0,
                    },
                    FixtureSample {
                        timestamp_ms: 240_000,
                        value: 2.0,
                    },
                    FixtureSample {
                        timestamp_ms: 300_000,
                        value: 3.0,
                    },
                ],
            },
            FixtureSeries {
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "m"),
                    crate::test_support::TestLabelPair::new("job", "b"),
                ])
                .into(),
                samples: vec![
                    FixtureSample {
                        timestamp_ms: 120_000,
                        value: 4.0,
                    },
                    FixtureSample {
                        timestamp_ms: 240_000,
                        value: 5.0,
                    },
                    FixtureSample {
                        timestamp_ms: 300_000,
                        value: 6.0,
                    },
                ],
            },
            FixtureSeries {
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "n"),
                    crate::test_support::TestLabelPair::new("job", "a"),
                ])
                .into(),
                samples: vec![
                    FixtureSample {
                        timestamp_ms: 120_000,
                        value: 7.0,
                    },
                    FixtureSample {
                        timestamp_ms: 240_000,
                        value: 8.0,
                    },
                    FixtureSample {
                        timestamp_ms: 300_000,
                        value: 9.0,
                    },
                ],
            },
        ] {
            push_float_series(&mut store, [fixture]);
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        // VALID families: each MUST plan to Some (never None, never Err).
        let valid: &[&str] = &[
            // Leaves / literals.
            "m",
            "42",
            "\"hello\"",
            "m offset 1m",
            "m @ 100",
            // Parenthesized.
            "(m)",
            "((m + 1))",
            // Unary.
            "-m",
            "- (m + 1)",
            // Binary: vector∘vector, vector∘scalar, scalar∘scalar, set ops.
            "m + n",
            "m * 2",
            "2 + 3",
            "m and n",
            "m or n",
            "m unless n",
            "m > 5",
            "m == bool 5",
            "sum(m) / sum(n)",
            // Simple aggregations.
            "sum(m)",
            "sum by (job) (m)",
            "avg without (job) (m)",
            "count(m)",
            "min(m)",
            "max(m)",
            "group(m)",
            // Param aggregations.
            "topk(1, m)",
            "bottomk(1, m)",
            "quantile(0.5, m)",
            "count_values(\"v\", m)",
            "stddev(m)",
            "stdvar(m)",
            "stddev by (job) (m)",
            // Rate-family + over_time range calls.
            "rate(m[5m])",
            "increase(m[5m])",
            "delta(m[5m])",
            "irate(m[5m])",
            "idelta(m[5m])",
            "avg_over_time(m[5m])",
            "sum_over_time(m[5m])",
            "count_over_time(m[5m])",
            "min_over_time(m[5m])",
            "max_over_time(m[5m])",
            "stddev_over_time(m[5m])",
            "stdvar_over_time(m[5m])",
            "last_over_time(m[5m])",
            "present_over_time(m[5m])",
            "quantile_over_time(0.5, m[5m])",
            // Aggregation over a range call (compositional).
            "sum by (job) (rate(m[5m]))",
            "max without (job) (avg_over_time(m[5m]))",
            // Scalar-math per-row calls.
            "abs(m)",
            "ceil(m)",
            "floor(m)",
            "round(m)",
            "round(m, 2)",
            "clamp(m, 1, 5)",
            "clamp_min(m, 2)",
            "clamp_max(m, 4)",
            "sqrt(m)",
            "exp(m)",
            "ln(m)",
            "log2(m)",
            "log10(m)",
            "sgn(m)",
            // Trig.
            "sin(m)",
            "cos(m)",
            "tan(m)",
            // Label ops.
            "label_replace(m, \"x\", \"y\", \"job\", \"(.*)\")",
            "label_join(m, \"x\", \"-\", \"job\")",
            "sort(m)",
            "sort_desc(m)",
            // Utilities.
            "time()",
            "pi()",
            "scalar(sum(m))",
            "vector(1)",
            "timestamp(m)",
            "absent(m)",
            "absent(nonexistent_metric)",
            "absent_over_time(m[5m])",
            "day_of_week()",
            "day_of_month(m)",
            "minute()",
            "hour()",
            // Histogram-quantile over a classic bucket vector (float series).
            "histogram_quantile(0.5, m)",
            // Top-level raw matrix selector / subquery (instant query).
            "m[5m]",
            "m[5m:1m]",
            "rate(m[5m:1m])",
            "sum_over_time(m[5m:1m])",
        ];
        for query in valid {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse valid `{query}`: {error}"));
            let planned = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("plan valid `{query}` errored: {error}"));
            assert2::assert!(planned.is_some());
        }

        // INVALID families: each MUST surface as Err (never Ok(None), never
        // Ok(Some)). These mirror the corpus `expect fail` cases that previously
        // deferred to the interpreter purely to raise the canonical error.
        let invalid: &[&str] = &[
            // Non-scalar / out-of-range / NaN scalar params.
            "quantile_over_time(m, m[5m])",
            "topk(m, m)",
            "quantile(m, m)",
            "clamp(m, m, 5)",
            "round(m, m)",
            "histogram_quantile(m, m)",
            // Non-string-literal label args.
            "label_replace(m, m, \"y\", \"job\", \"(.*)\")",
            "label_join(m, m, \"-\", \"job\")",
            "count_values(m, m)",
            "sort_by_label(m, m)",
            // Wrong arity.
            "time(m)",
            "pi(m)",
            "scalar(m, m)",
            "vector(m, m)",
            "timestamp(m, m)",
            "label_replace(m, \"x\")",
            "histogram_quantile(0.5)",
            // Type mismatch in a binary op (vector op range).
            "m + m[5m]",
        ];
        for query in invalid {
            let Ok(expr) =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            else {
                // A parse-time rejection is also a total outcome (never reaches
                // the planner), so a query the parser rejects is acceptable here.
                continue;
            };
            let outcome = engine.plan_instant_expr("t", &expr, time_ms).await;
            let _kind = match &outcome {
                Ok(Some(_)) => "Ok(Some)",
                Ok(None) => "Ok(None)",
                Err(_) => "Err",
            };
            assert2::assert!(outcome.is_err());
        }
    }
}
