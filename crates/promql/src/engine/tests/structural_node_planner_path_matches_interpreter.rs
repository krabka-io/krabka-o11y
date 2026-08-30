use super::*;

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
    for (lbls, ts, value) in [
        (
            labels(&[("__name__", "m"), ("job", "api")]),
            60_000_i64,
            2.0,
        ),
        (labels(&[("__name__", "m"), ("job", "api")]), 120_000, 3.0),
        (labels(&[("__name__", "m"), ("job", "db")]), 120_000, 7.0),
        // A genuine NaN latest-in-window sample (kept, negated to NaN).
        (
            labels(&[("__name__", "m"), ("job", "nan")]),
            120_000,
            f64::NAN,
        ),
        // A stale marker (dropped on both paths).
        (
            labels(&[("__name__", "m"), ("job", "stale")]),
            120_000,
            stale_bits,
        ),
        // A series with a short history for the matrix / subquery / smoothed
        // shapes.
        (labels(&[("__name__", "g")]), 0, 1.0),
        (labels(&[("__name__", "g")]), 60_000, 2.0),
        (labels(&[("__name__", "g")]), 120_000, 4.0),
        (labels(&[("__name__", "g")]), 180_000, 8.0),
        (labels(&[("__name__", "g")]), 240_000, 16.0),
    ] {
        store.push_float("t", lbls, ts, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries: &[(&str, i64)] = &[
        // Unary over a vector (drops `__name__`, negates each value, keeps a
        // genuine NaN, drops the stale marker).
        ("-m", 120_000),
        // Unary over an aggregate result (vector).
        ("-sum(m)", 120_000),
        // Unary over a scalar.
        ("-(1 + 2)", 120_000),
        // Double negation.
        ("- -m", 120_000),
        // Bare numeric / string literals.
        ("42", 120_000),
        ("-7.5", 120_000),
        (r#""hello""#, 120_000),
        // Raw matrix selector / subquery (RangeMatrix from query_instant).
        ("g[3m]", 240_000),
        ("m[2m]", 120_000),
        ("g[4m:1m]", 240_000),
        // `smoothed` extended selector (vector). The extension parser is not
        // feature-gated, so this routes in both build configs.
        ("smoothed(g)", 90_000),
    ];

    for &(query, time_ms) in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // The recursive planner must claim every one of these.
        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = sort_instant_result(
            engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap_or_else(|error| panic!("operator `{query}`: {error}")),
        );
        let via_interpreter = sort_instant_result(
            engine
                .eval_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}")),
        );

        assert2::assert!(query_results_match(&via_interpreter, &via_operators));
    }

    // Pin the result types the parity above relies on.
    for (query, time_ms, want) in [
        ("42", 120_000_i64, "scalar"),
        (r#""hello""#, 120_000, "string"),
        ("g[3m]", 240_000, "matrix"),
        ("g[4m:1m]", 240_000, "matrix"),
        ("-m", 120_000, "vector"),
        ("-(1 + 2)", 120_000, "scalar"),
    ] {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap();
        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let result = engine
            .assemble_planned_instant(plan, time_ms)
            .await
            .unwrap();
        assert2::assert!(result.result_type() == want);
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
