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
        super::super::range_expr_routes_through_planner(probe)
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
