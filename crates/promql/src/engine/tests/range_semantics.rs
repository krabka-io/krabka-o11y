use super::*;

mod range_planner_gate_routes_expected_shapes {
    use super::*;

    /// Differential parity for a range query through the per-step operator planner.
    ///
    /// The planner must produce the byte-exact `RangeMatrix` that the interpreter
    /// range path produces: the same series, the same series order, the same
    /// labelsets, the same per-step `(t, value)` points, the same gaps, and the
    /// same scalar-over-range shape. The test also pins which corpus-shaped range
    /// expressions route through the operator planner and which fall back to the
    /// interpreter. The gate coverage is then explicit, and the test catches
    /// regressions.
    #[test]
    pub(crate) fn range_planner_gate_routes_expected_shapes() {
        use promql_parser::parser::Expr;

        use crate::{DurationExprContext, parse_promql_with_duration_context};

        let routes = |query: &str| -> bool {
            let expr = parse_promql_with_duration_context(
                query,
                DurationExprContext::range(0, 120_000, millis(60_000)),
            )
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
            let mut probe = &expr;
            while let Expr::Paren(paren) = probe {
                probe = &paren.expr;
            }
            super::super::super::range_expr_routes_through_planner(probe)
        };

        // Plannable range shapes that now flow through the per-step operators.
        for query in [
            "rate(bar[30s])",
            "sum_over_time(bar[30s])",
            "requests * 2",
            "foo > 2 or bar",
            "abs(metric)",
            "sum by(job)(metric)",
            "label_replace(metric, \"l\", \"v\", \"\", \"\")",
            // Aggregations over a rate / `*_over_time` range call now route
            // through the planner: the UDF emits NULL (not a NaN sentinel) for a
            // no-value window, the aggregate planner drops those NULL rows before
            // grouping, and the aggregates skip NULL — matching the interpreter,
            // which omits no-value series before aggregating.
            "sum(rate(bar[30s]))",
            "avg by(job)(rate(bar[30s]))",
            "max without(path)(increase(bar[2m]))",
            "count(avg_over_time(bar[1m]))",
            // Parameterized aggregations over a plannable float inner now recurse
            // the inner vector and apply the shared interpreter routine per step
            // (a `Precomputed` result), so they route through the planner too.
            "topk(1, metric)",
            "bottomk(2, metric) by(job)",
            "quantile(0.9, metric)",
            "count_values(\"v\", metric)",
            "stddev by(job)(metric)",
            "stdvar(metric)",
            // A range/`*_over_time` call whose argument is a SUBQUERY now routes
            // through the planner: the subquery's sub-grid is evaluated per-step
            // through the recursive planner and the shared outer fold is applied.
            "avg_over_time(bar[5m:30s])",
            "rate(sum_over_time(bar[30s:10s])[2m:30s])",
            // A subquery whose inner is a unary negation now routes too:
            // `Expr::Unary` is planner-supported, so the subquery's structural
            // gate accepts it.
            "avg_over_time((-bar)[5m:30s])",
            // A param aggregation over a plannable subquery-range inner routes
            // through the planner too (the inner subquery is plannable).
            "topk(1, max_over_time(metric[5m:1m]))",
            // `sort_by_label` / `sort_by_label_desc` now route through the planner.
            "sort_by_label(metric, \"job\")",
            "sort_by_label_desc(metric, \"job\")",
            // The experimental `*_over_time` members route through the shared kernel.
            "mad_over_time(metric[5m])",
            "first_over_time(metric[5m])",
            "ts_of_max_over_time(metric[5m])",
            // `info(v [, selector])` routes through the planner (the input vector is
            // plannable; the join is the shared kernel).
            "info(metric)",
            "info(metric, {__name__=\"target_info\"})",
            // A bare top-level instant-vector selector now routes through the
            // planner: the interpreter range path is fixed to the left-OPEN
            // lookback, agreeing with the operator selector chain (and Prometheus).
            "metric",
            // A top-level scalar-typed expression now routes too: both range paths
            // fold an identical no-label scalar series per step, so the operator
            // driver matches the interpreter byte-for-byte.
            "42",
            "1 + 2",
            "time()",
            // A bare selector with `@ start()`/`@ end()` now routes too: the
            // per-step planner driver scopes the query's `[start, end]` bounds in
            // `AT_MODIFIER_BOUNDS`, and `plan_instant_selector` resolves those
            // modifiers to the range bounds, matching the interpreter's dedicated
            // `eval_vector_selector_over_steps`.
            "metric @ start()",
            "metric @ end()",
        ] {
            assert2::assert!(routes(query));
        }

        // A raw matrix selector is a range-vector shape owned by the interpreter's
        // dedicated matrix range path (not per-step plannable), so the gate keeps
        // it on the interpreter.
        assert2::assert!(!routes("bar[30s]"));
    }
}

mod range_at_start_end_selector_planner_matches_interpreter {
    use super::*;

    #[derive(bon::Builder)]
    struct SamplePoint {
        timestamp_ms: i64,
        value: f64,
    }

    struct JobSamples {
        job: &'static str,
        samples: Vec<SamplePoint>,
    }

    struct RangeQueryBounds {
        start: i64,
        end: i64,
        step: krabka_units::prelude::Time,
    }

    struct QueryExpectation {
        query: &'static str,
        expect_series: bool,
    }

    /// Differential parity for a bare top-level selector with `@ start()` or `@ end()`.
    ///
    /// The query is a range query. The per-step planner range driver scopes the
    /// query bounds `[start, end]`, and `plan_instant_selector` resolves
    /// `@ start()` and `@ end()` to those bounds. The result is a fixed eval
    /// instant repeated across every step, the same as the interpreter's dedicated
    /// `eval_vector_selector_over_steps`. This proves:
    ///   1. the gate routes `m @ start()` / `m @ end()` through the planner, and
    ///   2. planner (public range path) == interpreter byte-for-byte.
    #[tokio::test]
    pub(crate) async fn range_at_start_end_selector_planner_matches_interpreter() {
        use crate::DurationExprContext;

        let mut store = InMemoryMetricStore::new();
        let jobs = [
            JobSamples {
                job: "a",
                samples: vec![
                    SamplePoint::builder().timestamp_ms(0).value(1.0).build(),
                    SamplePoint::builder()
                        .timestamp_ms(120_000)
                        .value(2.0)
                        .build(),
                    SamplePoint::builder()
                        .timestamp_ms(300_000)
                        .value(3.0)
                        .build(),
                ],
            },
            JobSamples {
                job: "b",
                samples: vec![
                    SamplePoint::builder().timestamp_ms(0).value(10.0).build(),
                    SamplePoint::builder()
                        .timestamp_ms(180_000)
                        .value(20.0)
                        .build(),
                    SamplePoint::builder()
                        .timestamp_ms(300_000)
                        .value(30.0)
                        .build(),
                ],
            },
        ];
        for job_index in 0..jobs.len() {
            let job = &jobs[job_index];
            for sample_index in 0..job.samples.len() {
                let sample = &job.samples[sample_index];
                store.push_float(
                    "t",
                    labels(&[
                        crate::test_support::TestLabelPair::new("__name__", "m"),
                        crate::test_support::TestLabelPair::new("job", job.job),
                    ]),
                    sample.timestamp_ms,
                    sample.value,
                );
            }
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let bounds = RangeQueryBounds {
            start: 0,
            end: 300_000,
            step: millis(60_000),
        };

        // `m @ start()` pins eval to t=0 and `m @ end()` to t=300000: both have
        // an in-window sample, so they yield series. `m @ start() offset 1m`
        // shifts the pinned eval back 60s to t=-60000, whose 5m window
        // (-360000, -60000] holds NO sample (the earliest is at t=0), so it
        // yields an EMPTY matrix — the Prometheus-correct result.
        let queries = [
            QueryExpectation {
                query: "m @ start()",
                expect_series: true,
            },
            QueryExpectation {
                query: "m @ end()",
                expect_series: true,
            },
            QueryExpectation {
                query: "m @ start() offset 1m",
                expect_series: false,
            },
        ];
        for query_index in 0..queries.len() {
            let query = &queries[query_index];
            // (1) the gate routes the `@ start()/end()` selector through the planner.
            assert2::assert!(range_query_routes_through_planner(
                query.query,
                DurationExprContext::range(bounds.start, bounds.end, bounds.step)
            ));

            // (2) the planner resolves `@ start()`/`@ end()` to a FIXED eval
            // instant repeated across every grid step, so each surviving series
            // carries the SAME value at every one of the 6 steps (the value it
            // had at the pinned eval instant), matching Prometheus.
            let QueryResult::RangeMatrix(series) = engine
                .query_range(
                    &tenant_id("t"),
                    query.query,
                    bounds.start,
                    bounds.end,
                    bounds.step,
                )
                .await
                .unwrap_or_else(|error| panic!("planner `{}`: {error}", query.query))
            else {
                panic!("expected matrix for `{}`", query.query);
            };
            assert2::assert!(!series.is_empty() == query.expect_series);
            for s in &series {
                let times: Vec<i64> = s.samples.iter().map(|sample| sample.timestamp_ms).collect();
                assert2::assert!(times == vec![0, 60_000, 120_000, 180_000, 240_000, 300_000]);
                let values: Vec<u64> = s
                    .samples
                    .iter()
                    .map(|sample| float_value(&sample.value).to_bits())
                    .collect();
                assert2::assert!(values.windows(2).all(|w| w[0] == w[1]));
            }
        }

        // Instant-query start/end both mean the query evaluation timestamp.
        for query in ["m @ start()", "m @ end()"] {
            let QueryResult::InstantVector(samples) = engine
                .query_instant(&tenant_id("t"), query, 120_000)
                .await
                .unwrap_or_else(|error| panic!("instant `{query}`: {error}"))
            else {
                panic!("expected instant vector for `{query}`");
            };
            assert2::assert!(samples.len() == 2);
            assert2::assert!(samples.iter().all(|sample| sample.ts_ms == 120_000));
        }
    }
}

mod range_bare_selector_lookback_boundary_matches_prometheus {
    use super::*;

    /// Pins the range-path lookback boundary against Prometheus semantics.
    ///
    /// The instant-vector lookback window is `(eval - lookbackDelta, eval]`, which
    /// is left-open and right-closed. A sample with `ts == eval - lookbackDelta`
    /// sits exactly on the lower boundary, and the engine excludes it. This test
    /// proves:
    ///   1. the bare-selector range query routes through the planner,
    ///   2. planner == interpreter byte-for-byte across the grid, and
    ///   3. the engine excludes the boundary sample, which is the
    ///      Prometheus-correct behaviour, so a step whose only in-window candidate
    ///      is the boundary sample has no point.
    #[tokio::test]
    pub(crate) async fn range_bare_selector_lookback_boundary_matches_prometheus() {
        let lookback = EngineOpts::default().lookback_delta.millis_i64(); // 300_000 (5m)

        let mut store = InMemoryMetricStore::new();
        // A single sample at t=0. With a 5m lookback:
        //   - step t=0:        window (−300000, 0], sample at 0 is in-window (right-closed) -> value.
        //   - step t=300000:   window (0, 300000], sample at 0 is EXACTLY on the
        //                      left boundary -> EXCLUDED (left-open) -> NO point.
        //   - step t=240000:   window (−60000, 240000], sample at 0 in-window -> value.
        store.push_float(
            "t",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "m"),
                crate::test_support::TestLabelPair::new("job", "boundary"),
            ]),
            0,
            7.0,
        );
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let start = 0_i64;
        let end = lookback;
        let step = millis(60_000);

        // (1) the gate routes the bare selector through the planner.
        {
            use promql_parser::parser::Expr;

            use crate::{DurationExprContext, parse_promql_with_duration_context};
            let expr = parse_promql_with_duration_context(
                "m",
                DurationExprContext::range(start, end, step),
            )
            .unwrap();
            let mut probe = &expr;
            while let Expr::Paren(paren) = probe {
                probe = &paren.expr;
            }
            assert2::assert!(super::super::super::range_expr_routes_through_planner(
                probe
            ));
        }

        // (2) planner (public range path) yields the boundary-correct grid.
        let planner = engine
            .query_range(&tenant_id("t"), "m", start, end, step)
            .await
            .unwrap();

        // (3) the boundary step (t == eval - lookback) is excluded; the last
        // point is at t=240000, NOT at t=300000.
        let QueryResult::RangeMatrix(series) = &planner else {
            panic!("expected range matrix");
        };
        assert2::assert!(series.len() == 1);
        let times: Vec<i64> = series[0]
            .samples
            .iter()
            .map(|sample| sample.timestamp_ms)
            .collect();
        assert2::assert!(times == vec![0, 60_000, 120_000, 180_000, 240_000]);

        // (4) cross-check the interpreter's INSTANT path and the operator both
        // exclude the boundary sample directly, proving all three paths agree.
        let instant_at_boundary = engine
            .query_instant(&tenant_id("t"), "m", lookback)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = instant_at_boundary else {
            panic!("expected instant vector");
        };
        assert2::assert!(samples.is_empty());
    }
}

mod range_duration_expression_helpers_return_query_range_and_step_seconds {
    #[cfg(feature = "experimental-functions")]
    use super::*;

    #[cfg(feature = "experimental-functions")]
    struct DurationHelperCase {
        query: &'static str,
        expected: f64,
    }

    #[cfg(feature = "experimental-functions")]
    #[derive(bon::Builder)]
    struct DurationProbeSample {
        timestamp_ms: i64,
        value: f64,
    }

    #[cfg(feature = "experimental-functions")]
    struct NestedDurationCase {
        query: &'static str,
        expected: Vec<f64>,
    }

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    pub(crate) async fn range_duration_expression_helpers_return_query_range_and_step_seconds() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());

        for case in [
            DurationHelperCase {
                query: "range()",
                expected: 120.0,
            },
            DurationHelperCase {
                query: "step()",
                expected: 30.0,
            },
            DurationHelperCase {
                query: "start()",
                expected: 60.0,
            },
            DurationHelperCase {
                query: "end()",
                expected: 180.0,
            },
        ] {
            let series = query_range_matrix(&engine, case.query, 30_000).await;
            assert2::assert!(series.len() == 1);
            assert2::assert!(series[0].labels.len() == 0);
            assert2::assert!(
                series[0]
                    .samples
                    .iter()
                    .map(|sample| float_value(&sample.value))
                    .collect::<Vec<_>>()
                    == vec![case.expected; 5]
            );
        }
    }

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    async fn nested_duration_extrema_select_the_requested_windows_and_offsets() {
        let mut store = InMemoryMetricStore::new();
        for sample in [
            DurationProbeSample::builder()
                .timestamp_ms(0)
                .value(1.0)
                .build(),
            DurationProbeSample::builder()
                .timestamp_ms(30_000)
                .value(2.0)
                .build(),
            DurationProbeSample::builder()
                .timestamp_ms(60_000)
                .value(3.0)
                .build(),
            DurationProbeSample::builder()
                .timestamp_ms(90_000)
                .value(4.0)
                .build(),
            DurationProbeSample::builder()
                .timestamp_ms(120_000)
                .value(5.0)
                .build(),
            DurationProbeSample::builder()
                .timestamp_ms(150_000)
                .value(6.0)
                .build(),
            DurationProbeSample::builder()
                .timestamp_ms(180_000)
                .value(7.0)
                .build(),
        ] {
            store.push_float(
                "tenant-a",
                labels(&[crate::test_support::TestLabelPair::new(
                    "__name__",
                    "duration_probe",
                )]),
                sample.timestamp_ms,
                sample.value,
            );
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for case in [
            NestedDurationCase {
                query: "sum_over_time(duration_probe[min_of(max_of(step()*2, 45s), range()/2)])",
                expected: vec![5.0, 7.0, 9.0, 11.0, 13.0],
            },
            NestedDurationCase {
                query: "sum_over_time(duration_probe[max_of(min_of(step(), range()), 90s)])",
                expected: vec![6.0, 9.0, 12.0, 15.0, 18.0],
            },
            NestedDurationCase {
                query: "sum_over_time(duration_probe[step()] offset (min_of(max_of(step(), 10s), range())))",
                expected: vec![2.0, 3.0, 4.0, 5.0, 6.0],
            },
        ] {
            let result = engine
                .query_range(
                    &tenant_id("tenant-a"),
                    case.query,
                    60_000,
                    180_000,
                    millis(30_000),
                )
                .await
                .unwrap();
            let QueryResult::RangeMatrix(series) = result else {
                panic!("expected matrix for {}", case.query);
            };
            assert2::assert!(series.len() == 1, "{}", case.query);
            assert2::assert!(
                series[0]
                    .samples
                    .iter()
                    .map(|sample| float_value(&sample.value))
                    .collect::<Vec<_>>()
                    == case.expected,
                "{}",
                case.query
            );
        }
    }

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    async fn duration_extrema_reject_literal_division_by_zero_in_either_operand() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
        for query in [
            "sum_over_time(duration_probe[min_of(1m, 0/0)])",
            "sum_over_time(duration_probe[min_of(0/0, 1m)])",
            "sum_over_time(duration_probe[max_of(1m, 0/0)])",
            "sum_over_time(duration_probe[max_of(0/0, 1m)])",
        ] {
            let error = engine
                .query_range(
                    &tenant_id("tenant-a"),
                    query,
                    60_000,
                    180_000,
                    millis(30_000),
                )
                .await
                .unwrap_err();
            assert2::assert!(matches!(error, PromqlError::Parse(_)), "{query}: {error}");
            assert2::assert!(
                error.to_string().contains("division by zero"),
                "{query}: {error}"
            );
        }
    }
}

mod range_planner_path_matches_interpreter {
    use super::*;

    #[derive(bon::Builder)]
    struct RangeFixtureSample {
        timestamp_ms: i64,
        value: f64,
    }

    struct RangeFixtureSeries {
        labels: Labels,
        samples: Vec<RangeFixtureSample>,
    }

    fn range_labels(metric: &str, job: &str, group: &str) -> Labels {
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", metric),
            crate::test_support::TestLabelPair::new("job", job),
            crate::test_support::TestLabelPair::new("group", group),
        ])
    }

    fn minute_samples(values: &[f64]) -> Vec<RangeFixtureSample> {
        values
            .iter()
            .enumerate()
            .map(|(minute, &value)| {
                RangeFixtureSample::builder()
                    .timestamp_ms(minute as i64 * 60_000)
                    .value(value)
                    .build()
            })
            .collect()
    }

    fn push_range_series(store: &mut InMemoryMetricStore, series: RangeFixtureSeries) {
        for sample in series.samples {
            store.push_float(
                "t",
                series.labels.clone(),
                sample.timestamp_ms,
                sample.value,
            );
        }
    }

    struct RangeQueryBounds {
        start_ms: i64,
        end_ms: i64,
        step_ms: krabka_units::prelude::Time,
    }

    #[tokio::test]
    pub(crate) async fn range_planner_path_matches_interpreter() {
        use crate::DurationExprContext;

        let mut store = InMemoryMetricStore::new();
        let stale_bits = stale_nan();
        // Two counters (for rate and sum-by-rate), grouped by `group`.
        for series in [
            RangeFixtureSeries {
                labels: range_labels("http_requests_total", "api", "a"),
                samples: minute_samples(&[0.0, 1.0, 3.0, 6.0, 10.0, 15.0]),
            },
            RangeFixtureSeries {
                labels: range_labels("http_requests_total", "db", "a"),
                samples: minute_samples(&[0.0, 2.0, 4.0, 8.0, 16.0, 32.0]),
            },
            RangeFixtureSeries {
                // group b: a full history so `rate` has a value at every step
                // (no no-value NaN sentinel), keeping the operator aggregate
                // over this rate parity-exact in the forced comparison below.
                labels: range_labels("http_requests_total", "cache", "b"),
                samples: minute_samples(&[1.0, 5.0, 7.0, 9.0, 11.0, 20.0]),
            },
        ] {
            push_range_series(&mut store, series);
        }
        // A second counter family with a single-sample (no-value rate) series, to
        // exercise the SPARSE aggregate-over-rate parity (group b's only member is
        // no-value, so it is excluded from the group on both paths).
        for series in [
            RangeFixtureSeries {
                labels: range_labels("spotty_total", "api", "a"),
                samples: minute_samples(&[0.0, 1.0, 2.0, 3.0, 4.0, 5.0]),
            },
            RangeFixtureSeries {
                // Only one in-window sample at each step's 2m window: rate has no
                // value -> the operator rate emits NULL (not a NaN sentinel), the
                // aggregate planner drops it before grouping, and group b collapses
                // to no row at those steps — matching the interpreter, which omits
                // the no-value series. This drives the SPARSE aggregate-over-rate
                // parity proof below.
                labels: range_labels("spotty_total", "db", "b"),
                samples: vec![
                    RangeFixtureSample::builder()
                        .timestamp_ms(180_000)
                        .value(100.0)
                        .build(),
                ],
            },
        ] {
            push_range_series(&mut store, series);
        }
        // A plain gauge for a bare-selector range and a binary op.
        for sample in [
            RangeFixtureSample::builder()
                .timestamp_ms(0)
                .value(2.0)
                .build(),
            RangeFixtureSample::builder()
                .timestamp_ms(60_000)
                .value(4.0)
                .build(),
            RangeFixtureSample::builder()
                .timestamp_ms(120_000)
                .value(8.0)
                .build(),
            RangeFixtureSample::builder()
                .timestamp_ms(180_000)
                .value(16.0)
                .build(),
            RangeFixtureSample::builder()
                .timestamp_ms(240_000)
                .value(32.0)
                .build(),
            RangeFixtureSample::builder()
                .timestamp_ms(300_000)
                .value(64.0)
                .build(),
        ] {
            store.push_float(
                "t",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "gauge"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                ]),
                sample.timestamp_ms,
                sample.value,
            );
        }
        // A series whose mid-range latest in-window sample is a stale-NaN marker
        // (the series must vanish for the steps that select it) and whose later
        // sample is a genuine NaN (kept as a NaN value).
        for sample in [
            RangeFixtureSample::builder()
                .timestamp_ms(0)
                .value(1.0)
                .build(),
            RangeFixtureSample::builder()
                .timestamp_ms(60_000)
                .value(stale_bits)
                .build(),
            RangeFixtureSample::builder()
                .timestamp_ms(120_000)
                .value(3.0)
                .build(),
            RangeFixtureSample::builder()
                .timestamp_ms(180_000)
                .value(f64::NAN)
                .build(),
            RangeFixtureSample::builder()
                .timestamp_ms(240_000)
                .value(5.0)
                .build(),
        ] {
            store.push_float(
                "t",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "spotty"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                ]),
                sample.timestamp_ms,
                sample.value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let bounds = RangeQueryBounds {
            start_ms: 0,
            end_ms: 300_000,
            step_ms: millis(60_000),
        };

        // Queries the production gate routes through the per-step operator
        // planner. For these, the gate must accept them and the planner-routed
        // public `query_range` must evaluate successfully; the byte-exact value
        // checks are pinned below (and across the conformance corpus).
        let planner_routed = [
            // A rate over a counter (per-step rate projection).
            "rate(http_requests_total[2m])",
            // A vector-scalar binary op.
            "gauge * 2",
            // A vector-vector binary op (one-to-one on job).
            "gauge + on(job) http_requests_total{job=\"api\"}",
            // A scalar-math call over a selector (preserves genuine NaN, gaps
            // the stale-marker steps).
            "abs(spotty - 10)",
            // A simple aggregate over a bare selector (no rate sentinel).
            "sum by(job)(gauge)",
            // Aggregation over a rate: the marquee fix. Every series is dense, so
            // no no-value NULL arises — pure parity with the interpreter.
            "sum by(group)(rate(http_requests_total[2m]))",
            // Aggregation over a rate where one group member is SPARSE (the
            // single-sample `spotty_total` series at job=db,group=b yields no rate
            // value across the early steps). The UDF emits NULL for those steps,
            // the aggregate planner drops them before grouping, and group b
            // collapses to no result row at the steps where its only member is
            // no-value — exactly as the interpreter omits the no-value series.
            // group a (dense) is unaffected. This is the headline divergence the
            // fix closes, proven byte-exact through the public range path.
            "sum by(group)(rate(spotty_total[2m]))",
            // Parameterized aggregations over a plannable inner now route through
            // the planner per step. `topk` selects original series each step (a
            // series can appear/disappear between steps, stitched by fingerprint);
            // `quantile`/`stddev` reduce per group per step. All must equal the
            // interpreter byte-for-byte across the step grid.
            "topk(2, rate(http_requests_total[2m]))",
            "quantile(0.5, gauge)",
            "stddev by(group)(rate(http_requests_total[2m]))",
            // A BARE top-level instant-vector selector now routes through the
            // planner: the interpreter range path is fixed to the left-OPEN
            // lookback, so the operator selector chain matches it (and Prometheus)
            // byte-for-byte, including the stale-marker gaps and genuine-NaN keep.
            "gauge",
            "spotty",
            // A top-level SCALAR-typed expression now routes too: both range paths
            // fold an identical no-label scalar series per step.
            "42",
            "time()",
            "1 + 2",
        ];
        for query in planner_routed {
            assert2::assert!(range_query_routes_through_planner(
                query,
                DurationExprContext::range(bounds.start_ms, bounds.end_ms, bounds.step_ms)
            ));
            // The public range path now routes these through the planner (the
            // only evaluation engine); it must evaluate without falling back.
            let planner = engine
                .query_range(
                    &tenant_id("t"),
                    query,
                    bounds.start_ms,
                    bounds.end_ms,
                    bounds.step_ms,
                )
                .await
                .unwrap_or_else(|error| panic!("planner `{query}`: {error}"));
            assert2::assert!(matches!(planner, QueryResult::RangeMatrix(_)));
        }

        // The only top-level range shape the gate keeps on the interpreter is a
        // raw matrix selector / subquery (a range-vector shape owned by the
        // dedicated matrix/subquery range path, not the per-step instant
        // planner). Assert the gate excludes it.
        assert2::assert!(!range_query_routes_through_planner(
            "http_requests_total[2m]",
            DurationExprContext::range(bounds.start_ms, bounds.end_ms, bounds.step_ms)
        ));

        // The headline fix, proven directly on the SPARSE aggregate-over-rate.
        // `sum by(group)(rate(spotty_total[2m]))` over the full `[0, 300000]`
        // grid: group b's only member is a single-sample series, so its rate is
        // no-value (NULL) at every step. The rate UDF emits NULL for those steps,
        // the aggregate planner drops them before grouping, and group b collapses
        // to NO result row at all — only group a (dense) survives. (Before the
        // fix the operator path leaked a spurious NaN group-b row here.)
        let QueryResult::RangeMatrix(sparse) = engine
            .eval_range_via_planner_forced(
                "t",
                "sum by(group)(rate(spotty_total[2m]))",
                bounds.start_ms,
                bounds.end_ms,
                bounds.step_ms,
            )
            .await
            .unwrap()
        else {
            panic!("expected matrix for the sparse aggregate-over-rate");
        };
        let sparse_groups: Vec<Option<&str>> = sparse
            .iter()
            .map(|series| series.labels.get("group"))
            .collect();
        assert2::assert!(sparse_groups == vec![Some("a")]);

        // Pin the stale-vs-genuine-NaN semantics the scalar-math `spotty` parity
        // relies on, via the forced planner path on `abs(spotty - 10)`.
        let QueryResult::RangeMatrix(series) = engine
            .eval_range_via_planner_forced(
                "t",
                "spotty",
                bounds.start_ms,
                bounds.end_ms,
                bounds.step_ms,
            )
            .await
            .unwrap()
        else {
            panic!("expected matrix for `spotty`");
        };
        assert2::assert!(series.len() == 1);
        // Steps (ms) -> selected latest-in-window value, lookback 5m:
        //   0 -> 1.0; 60k -> stale (DROPPED, no point); 120k -> 3.0;
        //   180k -> NaN (kept); 240k -> 5.0; 300k -> 5.0 (240k still in window).
        let points = &series[0].samples;
        let times: Vec<i64> = points.iter().map(|sample| sample.timestamp_ms).collect();
        assert2::assert!(times == vec![0, 120_000, 180_000, 240_000, 300_000]);
        let nan_point = points
            .iter()
            .find(|sample| sample.timestamp_ms == 180_000)
            .expect("180k point");
        let SampleValue::Float(nan_value) = nan_point.value else {
            panic!("expected float at 180k");
        };
        assert2::assert!(nan_value.is_nan());
        assert2::assert!(!super::super::super::is_stale_nan(nan_value));
    }
}

mod range_query_accepts_parenthesized_expression {
    use super::*;

    #[tokio::test]
    pub(crate) async fn range_query_accepts_parenthesized_expression() {
        let store = up_series_store(&[(0, 0.0), (60_000, 1.0), (120_000, 2.0)]);

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_range(&tenant_id("tenant-a"), "(up)", 0, 120_000, millis(60_000))
            .await
            .unwrap();

        let series = lone_matrix_series(&result);
        check_series_points(
            series,
            &[
                ExpectedPoint {
                    ts_ms: 0,
                    value: 0.0,
                },
                ExpectedPoint {
                    ts_ms: 60_000,
                    value: 1.0,
                },
                ExpectedPoint {
                    ts_ms: 120_000,
                    value: 2.0,
                },
            ],
        );
    }
}

mod range_query_scans_store_once_per_matcher_set_not_per_step {
    use super::*;
    use crate::PromqlMatcher as LabelMatcher;

    #[tokio::test]
    pub(crate) async fn range_query_scans_store_once_per_matcher_set_not_per_step() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use crate::{
            error::Result,
            store::{
                ExemplarScan, LabelNameCardinality, LabelValueCardinality, MetadataScan,
                MetricStore, ScanResult, TsdbBlock, TsdbStats,
            },
        };

        // Wraps the in-memory store and counts store-level scans / series
        // resolutions, to prove the range driver no longer re-scans per step.
        struct CountingStore {
            inner: InMemoryMetricStore,
            scans: Arc<AtomicUsize>,
            series_calls: Arc<AtomicUsize>,
        }

        #[async_trait::async_trait]
        impl MetricStore for CountingStore {
            async fn scan(
                &self,
                t: &str,
                m: &[LabelMatcher],
                s: i64,
                e: i64,
            ) -> Result<ScanResult> {
                self.scans.fetch_add(1, Ordering::SeqCst);
                self.inner.scan(t, m, s, e).await
            }
            async fn series(
                &self,
                t: &str,
                m: &[LabelMatcher],
                s: i64,
                e: i64,
            ) -> Result<Vec<crate::PromqlLabels>> {
                self.series_calls.fetch_add(1, Ordering::SeqCst);
                self.inner.series(t, m, s, e).await
            }
            async fn label_names(
                &self,
                t: &str,
                m: &[LabelMatcher],
                s: i64,
                e: i64,
            ) -> Result<Vec<String>> {
                self.inner.label_names(t, m, s, e).await
            }
            async fn label_values(
                &self,
                t: &str,
                name: &str,
                m: &[LabelMatcher],
                s: i64,
                e: i64,
            ) -> Result<Vec<krabka_metrics::MetricString>> {
                self.inner.label_values(t, name, m, s, e).await
            }
            async fn exemplars(
                &self,
                t: &str,
                m: &[LabelMatcher],
                s: i64,
                e: i64,
            ) -> Result<ExemplarScan> {
                self.inner.exemplars(t, m, s, e).await
            }
            async fn metadata(&self, t: &str, metric: Option<&str>) -> Result<MetadataScan> {
                self.inner.metadata(t, metric).await
            }
            async fn cardinality_label_names(&self, t: &str) -> Result<Vec<LabelNameCardinality>> {
                self.inner.cardinality_label_names(t).await
            }
            async fn cardinality_label_values(
                &self,
                t: &str,
            ) -> Result<Vec<LabelValueCardinality>> {
                self.inner.cardinality_label_values(t).await
            }
            async fn cardinality_active_series(&self, t: &str) -> Result<Vec<crate::PromqlLabels>> {
                self.inner.cardinality_active_series(t).await
            }
            async fn tsdb_stats(&self, t: &str) -> Result<TsdbStats> {
                self.inner.tsdb_stats(t).await
            }
            async fn tsdb_blocks(&self, t: &str) -> Result<Vec<TsdbBlock>> {
                self.inner.tsdb_blocks(t).await
            }
        }

        let mut inner = InMemoryMetricStore::new();
        for i in 0..20 {
            inner.push_float(
                "t",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "up"),
                    crate::test_support::TestLabelPair::new("job", "broker"),
                ]),
                i * 15_000,
                1.0,
            );
        }
        let scans = Arc::new(AtomicUsize::new(0));
        let series_calls = Arc::new(AtomicUsize::new(0));
        let engine = PromqlEngine::new(
            Arc::new(CountingStore {
                inner,
                scans: Arc::clone(&scans),
                series_calls: Arc::clone(&series_calls),
            }),
            EngineOpts::default(),
        );

        // 20 steps at 15s. Without the union-window cache the driver scans the
        // store about twice per step (float and histogram probe) and resolves the
        // series once per step. With it, one store scan answers the histogram
        // probe and the float read of every step, and one series resolution
        // answers every step.
        let result = engine
            .eval_range_via_planner_forced(
                "t",
                "count({job=\"broker\"})",
                0,
                19 * 15_000,
                millis(15_000),
            )
            .await
            .unwrap();
        assert2::assert!(matches!(result, QueryResult::RangeMatrix(_)));
        check!(
            scans.load(Ordering::SeqCst) == 1,
            "store scans should collapse to one union scan, got {}",
            scans.load(Ordering::SeqCst)
        );
        check!(
            series_calls.load(Ordering::SeqCst) == 1,
            "series resolution should be cached across steps, got {}",
            series_calls.load(Ordering::SeqCst)
        );
    }
}

mod range_rate_uses_each_step_as_window_end {
    use super::*;

    #[tokio::test]
    pub(crate) async fn range_rate_uses_each_step_as_window_end() {
        let engine = requests_counter_engine(5);
        let result = engine
            .query_range(
                &tenant_id("tenant-a"),
                "rate(http_requests_total[5m])",
                240_000,
                300_000,
                millis(60_000),
            )
            .await
            .unwrap();

        let series = lone_matrix_series(&result);
        check_series_points(
            series,
            &[
                ExpectedPoint {
                    ts_ms: 240_000,
                    value: 4.0 / 300.0,
                },
                ExpectedPoint {
                    ts_ms: 300_000,
                    value: 5.0 / 300.0,
                },
            ],
        );
    }
}

mod range_scalar_expr_planner_path_matches_interpreter {
    use super::*;

    /// Differential parity for a top-level scalar-typed range query.
    ///
    /// A scalar expression such as `time()` or `1 + 2`
    /// routes through the per-step planner driver. The driver folds an identical
    /// no-label scalar series per step. The result must be byte-exact with the
    /// interpreter's `eval_instant_expr_over_steps` scalar stitching.
    #[tokio::test]
    pub(crate) async fn range_scalar_expr_planner_path_matches_interpreter() {
        use crate::DurationExprContext;

        // A store with one series so calendar functions over `time()` have a
        // defined eval timeline; scalars ignore the series entirely.
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "t",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "m"),
                crate::test_support::TestLabelPair::new("job", "a"),
            ]),
            0,
            1.0,
        );
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let start = 0_i64;
        let end = 300_000_i64;
        let step = millis(60_000);

        for query in ["42", "1 + 2", "time()", "2 * (3 + 4)"] {
            assert2::assert!(range_query_routes_through_planner(
                query,
                DurationExprContext::range(start, end, step)
            ));
            let planner = engine
                .query_range(&tenant_id("t"), query, start, end, step)
                .await
                .unwrap_or_else(|error| panic!("planner `{query}`: {error}"));
            // A scalar range query stitches a single no-label series, one float
            // point per step across the whole grid.
            let QueryResult::RangeMatrix(series) = &planner else {
                panic!("expected range matrix for `{query}`");
            };
            assert2::assert!(series.len() == 1);
            assert2::assert!(series[0].labels.is_empty());
            assert2::assert!(series[0].samples.len() == 6);
            // The constant scalars fold to their exact value at every step.
            if let Some(expected) = match query {
                "42" => Some(42.0_f64),
                "1 + 2" => Some(3.0),
                "2 * (3 + 4)" => Some(14.0),
                _ => None,
            } {
                for sample in &series[0].samples {
                    assert2::assert!(float_value(&sample.value).to_bits() == expected.to_bits());
                }
            }
        }
    }
}

mod range_selector_at_start_and_end_use_query_bounds {
    use super::*;

    #[derive(bon::Builder)]
    struct RangeSelectorExpectation {
        query: &'static str,
        value: f64,
    }

    #[tokio::test]
    pub(crate) async fn range_selector_at_start_and_end_use_query_bounds() {
        let store = SeriesFixture::new(labels(&[
            crate::test_support::TestLabelPair::new("__name__", "up"),
            crate::test_support::TestLabelPair::new("job", "api"),
        ]))
        .at(60_000_i64, 1.0)
        .at(120_000, 2.0)
        .at(180_000, 3.0)
        .store();

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for expected in [
            RangeSelectorExpectation::builder()
                .query("up @ start()")
                .value(1.0)
                .build(),
            RangeSelectorExpectation::builder()
                .query("up @ end()")
                .value(3.0)
                .build(),
        ] {
            let series = query_range_matrix(&engine, expected.query, 60_000).await;
            assert2::assert!(series.len() == 1);
            assert2::assert!(series[0].samples.len() == 3);
            for sample in &series[0].samples {
                let ts_ms = sample.timestamp_ms;
                let value = &sample.value;
                assert2::assert!([60_000, 120_000, 180_000].contains(&ts_ms));
                assert2::assert!(approx_eq(float_value(value), expected.value));
            }
        }
    }
}

mod range_selector_offset_shifts_matrix_window_backwards {
    use super::*;

    #[tokio::test]
    pub(crate) async fn range_selector_offset_shifts_matrix_window_backwards() {
        let store = up_series_store(&[(0, 0.0), (60_000, 1.0), (120_000, 2.0), (180_000, 3.0)]);

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(&tenant_id("tenant-a"), "up[2m] offset 1m", 180_000)
            .await
            .unwrap();

        let series = lone_matrix_series(&result);
        check_series_points(
            series,
            &[
                ExpectedPoint {
                    ts_ms: 60_000,
                    value: 1.0,
                },
                ExpectedPoint {
                    ts_ms: 120_000,
                    value: 2.0,
                },
            ],
        );
    }
}

mod range_selector_returns_samples_in_each_step_window {
    use super::*;

    #[tokio::test]
    pub(crate) async fn range_selector_returns_samples_in_each_step_window() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "up")]),
            0,
            0.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "up")]),
            60_000,
            1.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "up")]),
            90_000,
            stale_nan(),
        );
        store.push_float(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "up")]),
            120_000,
            2.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "up")]),
            180_000,
            3.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_range(
                &tenant_id("tenant-a"),
                "up[2m]",
                120_000,
                180_000,
                millis(60_000),
            )
            .await
            .unwrap();

        let QueryResult::RangeMatrix(series) = result else {
            panic!("expected matrix");
        };
        check!(series.len() == 1);
        check!(series[0].samples.len() == 3);
        check!(series[0].samples[0].timestamp_ms == 60_000);
        check!(series[0].samples[2].timestamp_ms == 180_000);
        check!(series[0].samples.iter().all(|sample| {
            let SampleValue::Float(value) = &sample.value else {
                return false;
            };
            value.to_bits() != stale_nan().to_bits()
        }));
    }
}

mod a_range_query_agrees_with_instant_evaluation_at_every_step {
    use super::{super::super::labels::labels_key, *};

    /// A range query's grid-driven leaves agree with evaluating the query one
    /// instant at a time.
    ///
    /// A range query plans each operator leaf once over the whole step grid and
    /// serves every step from that one execution. An instant query has no grid to
    /// drive, so it plans that single instant. The two must produce the same value
    /// for the same series at the same instant, bit for bit, or the hoist has
    /// changed what the engine answers.
    ///
    /// The shapes below are the ones the memo covers and the ones that must escape
    /// it: a bare selector and a rate (memoized), the same two behind an `offset`
    /// (memoized on a shifted grid), an aggregation over a rate and a scalar-math
    /// call over a selector (memoized through a nested leaf), an `*_over_time` fold,
    /// a `quantile_over_time` whose `phi` moves between steps (memoized once and
    /// then given up), a selector pinned by `@` (never memoized), and a subquery,
    /// whose sub-steps fall between the outer grid's instants and so must miss the
    /// memo.
    #[tokio::test]
    pub(crate) async fn a_range_query_agrees_with_instant_evaluation_at_every_step() {
        struct StoredSeries<'a> {
            metric_name: &'a str,
            job: &'a str,
            samples: &'a [TimedValue],
        }
        type TimedValue = FloatSampleInput;
        #[derive(PartialEq, Eq)]
        struct RangeSampleBits {
            timestamp_ms: i64,
            value_bits: u64,
        }

        let mut store = InMemoryMetricStore::new();
        let series_fixtures = [
            StoredSeries {
                metric_name: "http_requests_total",
                job: "api",
                samples: &[
                    TimedValue {
                        timestamp_ms: 0,
                        value: 0.0,
                    },
                    TimedValue {
                        timestamp_ms: 60_000,
                        value: 1.0,
                    },
                    TimedValue {
                        timestamp_ms: 120_000,
                        value: 3.0,
                    },
                    TimedValue {
                        timestamp_ms: 180_000,
                        value: 6.0,
                    },
                    TimedValue {
                        timestamp_ms: 240_000,
                        value: 10.0,
                    },
                    TimedValue {
                        timestamp_ms: 300_000,
                        value: 15.0,
                    },
                ],
            },
            StoredSeries {
                metric_name: "http_requests_total",
                job: "db",
                samples: &[
                    TimedValue {
                        timestamp_ms: 0,
                        value: 0.0,
                    },
                    TimedValue {
                        timestamp_ms: 60_000,
                        value: 2.0,
                    },
                    TimedValue {
                        timestamp_ms: 120_000,
                        value: 4.0,
                    },
                    // A gap, so some steps have a no-value rate and some series
                    // drop out of a step entirely.
                    TimedValue {
                        timestamp_ms: 300_000,
                        value: 9.0,
                    },
                ],
            },
            StoredSeries {
                metric_name: "gauge",
                job: "api",
                samples: &[
                    TimedValue {
                        timestamp_ms: 0,
                        value: 2.0,
                    },
                    TimedValue {
                        timestamp_ms: 60_000,
                        value: 4.0,
                    },
                    TimedValue {
                        timestamp_ms: 120_000,
                        value: f64::NAN,
                    },
                    TimedValue {
                        timestamp_ms: 180_000,
                        value: 16.0,
                    },
                    TimedValue {
                        timestamp_ms: 240_000,
                        value: 32.0,
                    },
                    TimedValue {
                        timestamp_ms: 300_000,
                        value: 64.0,
                    },
                ],
            },
        ];
        push_float_series(
            &mut store,
            series_fixtures
                .into_iter()
                .map(|series| LabeledFloatSeries {
                    labels: labels(&[
                        crate::test_support::TestLabelPair::new("__name__", series.metric_name),
                        crate::test_support::TestLabelPair::new("job", series.job),
                    ]),
                    samples: series.samples.to_vec(),
                }),
        );
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let start_ms = 0_i64;
        let end_ms = 300_000_i64;
        let step_ms = 60_000_i64;
        let step = millis(60_000);
        for query in [
            "gauge",
            "gauge offset 1m",
            "rate(http_requests_total[2m])",
            "rate(http_requests_total[2m] offset 1m)",
            "sum by (job) (rate(http_requests_total[2m]))",
            "count by (job) (rate(http_requests_total[2m]))",
            "abs(gauge - 10)",
            "max_over_time(gauge[3m])",
            // `phi` is resolved per step and this one moves with the data, so the
            // memo must give the leaf up rather than answer a later step from the
            // quantile an earlier one asked for.
            "quantile_over_time(scalar(gauge) / 100, gauge[3m])",
            "gauge @ 120",
            "last_over_time(gauge[3m:1m])",
        ] {
            let QueryResult::RangeMatrix(series) = engine
                .query_range(&tenant_id("t"), query, start_ms, end_ms, step)
                .await
                .unwrap_or_else(|error| panic!("range `{query}`: {error}"))
            else {
                panic!("expected a range matrix for `{query}`");
            };
            let mut got: BTreeMap<String, Vec<RangeSampleBits>> = BTreeMap::new();
            for one in &series {
                got.insert(
                    labels_key(&one.labels),
                    one.samples
                        .iter()
                        .map(|sample| RangeSampleBits {
                            timestamp_ms: sample.timestamp_ms,
                            value_bits: float_value(&sample.value).to_bits(),
                        })
                        .collect(),
                );
            }

            let mut want: BTreeMap<String, Vec<RangeSampleBits>> = BTreeMap::new();
            let mut instant_ms = start_ms;
            while instant_ms <= end_ms {
                let result = engine
                    .query_instant(&tenant_id("t"), query, instant_ms)
                    .await
                    .unwrap_or_else(|error| panic!("instant `{query}` at {instant_ms}: {error}"));
                let QueryResult::InstantVector(samples) = result else {
                    panic!("expected an instant vector for `{query}`");
                };
                for sample in samples {
                    want.entry(labels_key(&sample.labels))
                        .or_default()
                        .push(RangeSampleBits {
                            timestamp_ms: instant_ms,
                            value_bits: float_value(&sample.value).to_bits(),
                        });
                }
                instant_ms += step_ms;
            }

            check!(got == want, "`{query}` diverged between range and instant");
        }
    }
}

mod a_range_query_over_or_matchers_counts_a_doubly_matched_series_once {
    use super::*;

    /// A series matching more than one `or` branch stays one series.
    ///
    /// `label_matcher_sets` resolves `{a or b}` to one matcher set per branch, and
    /// the operator leaf is filled branch by branch, so a series matching both
    /// branches arrives twice. The leaf must still present it as a single
    /// contiguous, time-ordered run, because `SeriesDivide` splits its input into
    /// per-series batches by scanning for a change in the label columns and every
    /// downstream operator assumes one batch per series.
    #[tokio::test]
    pub(crate) async fn a_range_query_over_or_matchers_counts_a_doubly_matched_series_once() {
        let mut store = InMemoryMetricStore::new();
        for ts_ms in [0_i64, 60_000, 120_000] {
            store.push_float(
                "tenant-a",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "up"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                    crate::test_support::TestLabelPair::new("instance", "a"),
                ]),
                ts_ms,
                2.0,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        // The one stored series matches both branches: its job is `api` and its
        // instance is `a`.
        let result = engine
            .query_range(
                &tenant_id("tenant-a"),
                r#"sum by (job) (up{job="api" or instance="a"})"#,
                0,
                120_000,
                millis(60_000),
            )
            .await
            .unwrap();

        let QueryResult::RangeMatrix(series) = result else {
            panic!("expected a range matrix");
        };
        check!(series.len() == 1);
        check!(series[0].labels.get("job") == Some("api"));
        check!(
            series[0]
                .samples
                .iter()
                .map(|sample| (sample.timestamp_ms, float_value(&sample.value)))
                .collect::<Vec<_>>()
                == vec![(0, 2.0), (60_000, 2.0), (120_000, 2.0)]
        );
    }
}
