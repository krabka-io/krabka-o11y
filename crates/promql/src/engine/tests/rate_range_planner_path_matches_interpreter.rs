use super::*;

struct RateRangeCase {
    query: &'static str,
    time_ms: i64,
}

#[tokio::test]
pub(crate) async fn rate_range_planner_path_matches_interpreter() {
    use promql_parser::parser::Expr;

    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // Float-only counters with a reset, a gauge for delta, an offset
    // history, and a single-sample series (no rate value).
    let mut store = InMemoryMetricStore::new();
    push_float_series(
        &mut store,
        [
            LabeledFloatSeries {
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                ]),
                samples: vec![
                    FloatSampleInput {
                        timestamp_ms: 0,
                        value: 0.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 60_000,
                        value: 1.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 120_000,
                        value: 2.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 180_000,
                        value: 3.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 240_000,
                        value: 4.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 300_000,
                        value: 5.0,
                    },
                ],
            },
            LabeledFloatSeries {
                // A counter reset mid-window (5 -> 1) exercises reset correction.
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
                    crate::test_support::TestLabelPair::new("job", "db"),
                ]),
                samples: vec![
                    FloatSampleInput {
                        timestamp_ms: 0,
                        value: 0.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 60_000,
                        value: 3.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 120_000,
                        value: 5.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 180_000,
                        value: 1.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 240_000,
                        value: 4.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 300_000,
                        value: 8.0,
                    },
                ],
            },
            LabeledFloatSeries {
                // A gauge with ups and downs for delta/idelta.
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "temperature"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                ]),
                samples: vec![
                    FloatSampleInput {
                        timestamp_ms: 180_000,
                        value: 10.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 240_000,
                        value: 7.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 300_000,
                        value: 9.0,
                    },
                ],
            },
            LabeledFloatSeries {
                // Single sample in-window: rate-family yields no value. Both paths
                // must DROP this series identically (NULL-drop on the operator
                // path, no-value omission on the interpreter).
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
                    crate::test_support::TestLabelPair::new("job", "lonely"),
                ]),
                samples: vec![FloatSampleInput {
                    timestamp_ms: 295_000,
                    value: 100.0,
                }],
            },
            LabeledFloatSeries {
                // A gauge whose window holds a GENUINE NaN sample: `delta` computes
                // a value (the window is non-empty with >=2 samples), and the
                // arithmetic yields NaN. That NaN is a real value (non-null), so it
                // must be KEPT and propagated on both paths — not dropped.
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "nan_gauge"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                ]),
                samples: vec![
                    FloatSampleInput {
                        timestamp_ms: 240_000,
                        value: f64::NAN,
                    },
                    FloatSampleInput {
                        timestamp_ms: 300_000,
                        value: 5.0,
                    },
                ],
            },
        ],
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries = [
        RateRangeCase {
            query: "rate(http_requests_total[5m])",
            time_ms: 300_000,
        },
        RateRangeCase {
            query: "increase(http_requests_total[5m])",
            time_ms: 300_000,
        },
        RateRangeCase {
            query: "delta(temperature[2m])",
            time_ms: 300_000,
        },
        RateRangeCase {
            query: "irate(http_requests_total[5m])",
            time_ms: 300_000,
        },
        RateRangeCase {
            query: "idelta(http_requests_total[5m])",
            time_ms: 300_000,
        },
        // @ and offset on the matrix selector exercise the time modifier.
        RateRangeCase {
            query: "rate(http_requests_total[3m] @ 300)",
            time_ms: 999_999,
        },
        RateRangeCase {
            query: "increase(http_requests_total[4m] offset 1m)",
            time_ms: 360_000,
        },
        // Tighter window that strands the single-sample series.
        RateRangeCase {
            query: "rate(http_requests_total{job=\"api\"}[90s])",
            time_ms: 300_000,
        },
        // A genuine-NaN delta: the computed value is NaN but non-null, so the
        // series is KEPT (not dropped). Both paths must agree, NaN-aware.
        RateRangeCase {
            query: "delta(nan_gauge[2m])",
            time_ms: 300_000,
        },
    ];

    for query_index in 0..queries.len() {
        let case = &queries[query_index];
        let query = case.query;
        let time_ms = case.time_ms;
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let Expr::Call(_) = &expr else {
            panic!("`{query}` did not parse to a call");
        };
        let call = match_rate_range_call(&expr)
            .unwrap_or_else(|| panic!("`{query}` is not an operator-path rate call"));

        let interpreter = engine
            .eval_instant_call(
                "t",
                match &expr {
                    Expr::Call(call) => call,
                    _ => unreachable!(),
                },
                time_ms,
            )
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
        let planner = engine
            .eval_rate_range_via_planner(
                "t",
                MatrixSelectorAt {
                    selector: call.selector,
                    time_ms,
                },
                call.kind,
            )
            .await
            .unwrap_or_else(|error| panic!("planner `{query}`: {error}"));

        let normalize = |result: QueryResult| fingerprint_sorted(result, query);

        let interpreter = normalize(interpreter);
        let planner = normalize(planner);
        // NaN-aware comparison so a genuine NaN value (e.g. `delta(nan_gauge)`)
        // is treated as equal to itself across both paths rather than spuriously
        // failing under IEEE `NaN != NaN`.
        assert2::assert!(instant_samples_match(&interpreter, &planner));

        // Pin that the genuine-NaN delta is KEPT (non-null NaN value), not
        // dropped as if it were a no-value series.
        if query == "delta(nan_gauge[2m])" {
            assert2::assert!(planner.len() == 1);
            let value = float_value(&planner[0].value);
            assert2::assert!(value.is_nan());
        }
    }
}
