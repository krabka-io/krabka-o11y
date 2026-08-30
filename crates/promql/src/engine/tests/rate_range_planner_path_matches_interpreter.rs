use super::*;

#[tokio::test]
pub(crate) async fn rate_range_planner_path_matches_interpreter() {
    use promql_parser::parser::Expr;

    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // Float-only counters with a reset, a gauge for delta, an offset
    // history, and a single-sample series (no rate value).
    let mut store = InMemoryMetricStore::new();
    for (lbls, samples) in [
        (
            labels(&[("__name__", "http_requests_total"), ("job", "api")]),
            vec![
                (0_i64, 0.0),
                (60_000, 1.0),
                (120_000, 2.0),
                (180_000, 3.0),
                (240_000, 4.0),
                (300_000, 5.0),
            ],
        ),
        (
            // A counter reset mid-window (5 -> 1) exercises reset correction.
            labels(&[("__name__", "http_requests_total"), ("job", "db")]),
            vec![
                (0, 0.0),
                (60_000, 3.0),
                (120_000, 5.0),
                (180_000, 1.0),
                (240_000, 4.0),
                (300_000, 8.0),
            ],
        ),
        (
            // A gauge with ups and downs for delta/idelta.
            labels(&[("__name__", "temperature"), ("job", "api")]),
            vec![(180_000, 10.0), (240_000, 7.0), (300_000, 9.0)],
        ),
        (
            // Single sample in-window: rate-family yields no value. Both paths
            // must DROP this series identically (NULL-drop on the operator
            // path, no-value omission on the interpreter).
            labels(&[("__name__", "http_requests_total"), ("job", "lonely")]),
            vec![(295_000, 100.0)],
        ),
        (
            // A gauge whose window holds a GENUINE NaN sample: `delta` computes
            // a value (the window is non-empty with >=2 samples), and the
            // arithmetic yields NaN. That NaN is a real value (non-null), so it
            // must be KEPT and propagated on both paths — not dropped.
            labels(&[("__name__", "nan_gauge"), ("job", "api")]),
            vec![(240_000, f64::NAN), (300_000, 5.0)],
        ),
    ] {
        for (ts, value) in samples {
            store.push_float("t", lbls.clone(), ts, value);
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries = [
        ("rate(http_requests_total[5m])", 300_000_i64),
        ("increase(http_requests_total[5m])", 300_000),
        ("delta(temperature[2m])", 300_000),
        ("irate(http_requests_total[5m])", 300_000),
        ("idelta(http_requests_total[5m])", 300_000),
        // @ and offset on the matrix selector exercise the time modifier.
        ("rate(http_requests_total[3m] @ 300)", 999_999),
        ("increase(http_requests_total[4m] offset 1m)", 360_000),
        // Tighter window that strands the single-sample series.
        ("rate(http_requests_total{job=\"api\"}[90s])", 300_000),
        // A genuine-NaN delta: the computed value is NaN but non-null, so the
        // series is KEPT (not dropped). Both paths must agree, NaN-aware.
        ("delta(nan_gauge[2m])", 300_000),
    ];

    for (query, time_ms) in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let Expr::Call(_) = &expr else {
            panic!("`{query}` did not parse to a call");
        };
        let (selector, kind) = match_rate_range_call(&expr)
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
            .eval_rate_range_via_planner("t", selector, time_ms, kind)
            .await
            .unwrap_or_else(|error| panic!("planner `{query}`: {error}"));

        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };

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
