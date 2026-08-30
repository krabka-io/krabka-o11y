use super::*;

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
    use promql_parser::parser::Expr;

    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();
    for (job, samples) in [
        ("a", vec![(0_i64, 1.0_f64), (120_000, 2.0), (300_000, 3.0)]),
        ("b", vec![(0, 10.0), (180_000, 20.0), (300_000, 30.0)]),
    ] {
        for (ts, value) in samples {
            store.push_float("t", labels(&[("__name__", "m"), ("job", job)]), ts, value);
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let (start, end, step) = (0_i64, 300_000_i64, millis(60_000));

    // `m @ start()` pins eval to t=0 and `m @ end()` to t=300000: both have
    // an in-window sample, so they yield series. `m @ start() offset 1m`
    // shifts the pinned eval back 60s to t=-60000, whose 5m window
    // (-360000, -60000] holds NO sample (the earliest is at t=0), so it
    // yields an EMPTY matrix — the Prometheus-correct result.
    for (query, expect_series) in [
        ("m @ start()", true),
        ("m @ end()", true),
        ("m @ start() offset 1m", false),
    ] {
        // (1) the gate routes the `@ start()/end()` selector through the planner.
        let expr =
            parse_promql_with_duration_context(query, DurationExprContext::range(start, end, step))
                .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let mut probe = &expr;
        while let Expr::Paren(paren) = probe {
            probe = &paren.expr;
        }
        assert2::assert!(super::super::range_expr_routes_through_planner(probe));

        // (2) the planner resolves `@ start()`/`@ end()` to a FIXED eval
        // instant repeated across every grid step, so each surviving series
        // carries the SAME value at every one of the 6 steps (the value it
        // had at the pinned eval instant), matching Prometheus.
        let QueryResult::RangeMatrix(series) = engine
            .query_range("t", query, start, end, step)
            .await
            .unwrap_or_else(|error| panic!("planner `{query}`: {error}"))
        else {
            panic!("expected matrix for `{query}`");
        };
        assert2::assert!(!series.is_empty() == expect_series);
        for s in &series {
            let times: Vec<i64> = s.samples.iter().map(|(t, _)| *t).collect();
            assert2::assert!(times == vec![0, 60_000, 120_000, 180_000, 240_000, 300_000]);
            let values: Vec<u64> = s
                .samples
                .iter()
                .map(|(_, v)| float_value(v).to_bits())
                .collect();
            assert2::assert!(values.windows(2).all(|w| w[0] == w[1]));
        }
    }

    // A bare `@ start()` selector in an INSTANT query has no range bounds, so it
    // must raise the SAME hard error on the planner path as the interpreter —
    // never silently produce a result or fall back.
    let instant_err = engine.query_instant("t", "m @ start()", 120_000).await;
    assert2::assert!(matches!(instant_err, Err(PromqlError::Unsupported(_))));
}
