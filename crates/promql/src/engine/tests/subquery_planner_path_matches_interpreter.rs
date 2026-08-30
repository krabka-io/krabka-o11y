use super::*;

/// Differential parity for subqueries routed through the recursive planner.
///
/// The query is a range call or a `*_over_time` call whose argument is
/// `inner[range:res]`. The planner builds the subquery range vector per
/// aligned sub-step and then applies the shared outer fold. The result must
/// equal the interpreter's `eval_subquery` plus outer fold byte-for-byte, with
/// a NaN-aware comparison.
#[tokio::test]
pub(crate) async fn subquery_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A float-only store exercising the subquery sub-grid:
    //  - `reqs_total{l}`: two counters (l=a, l=b) for rate/over_time-of-rate.
    //  - `gauge{l}`: a plain gauge in two label groups for the aggregating
    //    inner (`sum by(l)`).
    //  - `sparse{l}`: a series with a single early sample so a tight subquery
    //    window strands it (no-value sub-grid -> dropped series), plus a dense
    //    member so the surviving series is observable.
    let mut store = InMemoryMetricStore::new();
    // Counters: slope = factor over 60s, sampled every 30s out to 20m.
    for (l, factor) in [("a", 1.0), ("b", 2.0)] {
        let lbls = labels(&[("__name__", "reqs_total"), ("l", l)]);
        for step in 0..=40_i32 {
            store.push_float(
                "t",
                lbls.clone(),
                i64::from(step) * 30_000,
                f64::from(step) * factor,
            );
        }
    }
    // Gauges in two groups (`sum by(l)` collapses the `g` member dimension).
    for (l, g, base) in [
        ("a", "0", 3.0),
        ("a", "1", 5.0),
        ("b", "0", 7.0),
        ("b", "1", 11.0),
    ] {
        let lbls = labels(&[("__name__", "gauge"), ("l", l), ("g", g)]);
        for step in 0..=40_i32 {
            store.push_float(
                "t",
                lbls.clone(),
                i64::from(step) * 30_000,
                base + f64::from(step),
            );
        }
    }
    // Sparse: l=dense has a full history; l=stranded has only one early
    // sample, so a tight late subquery window yields it no sub-grid points.
    {
        let dense = labels(&[("__name__", "sparse"), ("l", "dense")]);
        for step in 0..=40_i32 {
            store.push_float(
                "t",
                dense.clone(),
                i64::from(step) * 30_000,
                f64::from(step),
            );
        }
        let stranded = labels(&[("__name__", "sparse"), ("l", "stranded")]);
        store.push_float("t", stranded, 0, 1.0);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Each query must route through the operator path and match the
    // interpreter byte-for-byte. `EngineOpts::default().eval_interval` is
    // 60s, so a subquery written `[range:]` (no resolution) uses a 60s stride
    // on BOTH paths.
    let queries = [
        // Selector inner, explicit resolution.
        ("rate(reqs_total[5m:1m])", 1_200_000_i64),
        // Nested: `*_over_time` over a `rate(...)` subquery — the inner rate is
        // itself planned per sub-step.
        ("max_over_time(rate(reqs_total[1m])[10m:2m])", 1_200_000),
        // Aggregating inner with DEFAULT resolution (`[5m:]` -> 60s stride).
        ("sum_over_time((sum by(l)(gauge))[5m:])", 1_200_000),
        ("avg_over_time((sum by(l)(gauge))[5m:])", 1_200_000),
        // `@` and offset on the subquery shift the evaluated end (and the
        // step-aligned start) identically on both paths.
        ("sum_over_time(gauge[5m:1m] @ 600)", 1_200_000),
        ("sum_over_time(gauge[5m:1m] offset 5m)", 1_200_000),
        // Sparse: the stranded member yields an empty sub-grid window and is
        // dropped from the result; the dense member survives. A tight window
        // at a late time.
        ("sum_over_time(sparse[1m:30s])", 1_200_000),
        ("last_over_time(sparse[1m:30s])", 1_200_000),
        // Binary inner.
        ("rate((reqs_total + reqs_total)[5m:1m])", 1_200_000),
        // Unary-negation inner: `Expr::Unary` now routes through the planner,
        // so the subquery's structural gate accepts it and the inner negation
        // is planned per sub-step.
        ("sum_over_time((-gauge)[5m:1m])", 1_200_000),
        ("max_over_time((-gauge)[5m:1m])", 1_200_000),
    ];

    for (query, time_ms) in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // Operator path: the recursive planner must claim this query.
        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(plan, time_ms)
            .await
            .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

        // Interpreter path: evaluate the same expression directly.
        let via_interpreter = engine
            .eval_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };

        let via_interpreter = normalize(via_interpreter);
        let via_operators = normalize(via_operators);
        assert2::assert!(instant_samples_match(&via_interpreter, &via_operators));

        // Pin the sparse-window rule: the stranded member is dropped (no
        // sub-grid points), so only the dense series survives.
        if query == "sum_over_time(sparse[1m:30s])" {
            assert2::assert!(via_operators.len() == 1);
            assert2::assert!(via_operators[0].labels.get("l") == Some("dense"));
        }
    }
}
