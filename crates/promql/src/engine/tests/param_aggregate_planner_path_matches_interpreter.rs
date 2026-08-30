use super::*;

#[tokio::test]
pub(crate) async fn param_aggregate_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A float-only store exercising the parameterized aggregations:
    //  - `m{job,instance}`: a multi-instance gauge per job, with a TIE between
    //    two instances (api/0 and api/1 both 5.0) so topk/bottomk tie-breaks
    //    (by `labels_key`) are observable, plus a genuine NaN member to pin
    //    NaN ordering (`total_cmp`) and quantile/stddev NaN handling.
    //  - `single{instance}`: a one-member group (single-element quantile and
    //    stddev/stdvar -> 0).
    //  - `cv{instance}`: a metric with repeated values for `count_values`
    //    (two members share value 1, one is 2).
    //  - `reqs_total`: counters for the nested `topk(.., rate(...))` case.
    let mut store = InMemoryMetricStore::new();
    for (job, instance, value) in [
        ("api", "0", 5.0),
        ("api", "1", 5.0), // ties with api/0 under topk/bottomk
        ("api", "2", 2.0),
        ("api", "3", 8.0),
        ("api", "4", f64::NAN), // genuine NaN member
        ("db", "0", 1.0),
        ("db", "1", 9.0),
    ] {
        let lbls = labels(&[("__name__", "m"), ("job", job), ("instance", instance)]);
        store.push_float("t", lbls, 120_000, value);
    }
    // A single-member group per job (single-element quantile/stddev/stdvar).
    for (job, value) in [("api", 4.0), ("db", 7.0)] {
        let lbls = labels(&[("__name__", "single"), ("job", job)]);
        store.push_float("t", lbls, 120_000, value);
    }
    // Repeated values for count_values: 1, 1, 2 within job=api.
    for (instance, value) in [("0", 1.0), ("1", 1.0), ("2", 2.0)] {
        let lbls = labels(&[("__name__", "cv"), ("job", "api"), ("instance", instance)]);
        store.push_float("t", lbls, 120_000, value);
    }
    // Counters for `topk(.., rate(...))` (slope = factor / 60s).
    for (path, factor) in [("a", 1.0), ("b", 2.0), ("c", 5.0)] {
        let lbls = labels(&[("__name__", "reqs_total"), ("job", "api"), ("path", path)]);
        for step in 0..=3_i32 {
            store.push_float(
                "t",
                lbls.clone(),
                i64::from(step) * 60_000,
                f64::from(step) * factor,
            );
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Each query must route through the operator path and match the
    // interpreter byte-for-byte (NaN-aware, bit-exact).
    let queries = [
        // topk/bottomk: original series kept (labels incl. __name__), tie-break
        // by labels_key, k clamping (k > group size, k = 0). by and without.
        ("topk(2, m)", 120_000_i64),
        ("bottomk(2, m)", 120_000),
        ("topk(2, m) by (job)", 120_000),
        ("bottomk(2, m) by (job)", 120_000),
        ("topk(2, m) without (instance)", 120_000),
        // k larger than a group's size clamps to the whole group.
        ("topk(10, m) by (job)", 120_000),
        // k = 0 yields the empty vector.
        ("topk(0, m)", 120_000),
        ("bottomk(0, m) by (job)", 120_000),
        // Ties across the whole vector: api/0 and api/1 both 5.0.
        ("topk(3, m)", 120_000),
        // quantile: phi = 0 / 0.5 / 0.9 / 1, by and without.
        ("quantile(0, m) by (job)", 120_000),
        ("quantile(0.5, m) by (job)", 120_000),
        ("quantile(0.9, m) by (job)", 120_000),
        ("quantile(1, m) by (job)", 120_000),
        ("quantile(0.5, m) without (instance)", 120_000),
        // Single-element group: quantile equals the lone value.
        ("quantile(0.5, single) by (job)", 120_000),
        // count_values: one series per distinct value, value -> label, count.
        (r#"count_values("v", cv)"#, 120_000),
        (r#"count_values("v", cv) by (job)"#, 120_000),
        (r#"count_values("v", cv) without (instance)"#, 120_000),
        // stddev/stdvar: population std-dev / variance, by and without.
        ("stddev(m) by (job)", 120_000),
        ("stdvar(m) by (job)", 120_000),
        ("stddev(m) without (instance)", 120_000),
        ("stdvar without (instance) (m)", 120_000),
        // Single-element group -> stddev/stdvar = 0.
        ("stddev(single) by (job)", 120_000),
        ("stdvar(single) by (job)", 120_000),
        // No modifier (collapse all).
        ("stddev(m)", 120_000),
        ("quantile(0.5, m)", 120_000),
        // Nested: a parameterized aggregation over a rate inner already on the
        // operator path.
        ("topk(1, rate(reqs_total[3m]))", 180_000),
        ("quantile(0.5, rate(reqs_total[3m]))", 180_000),
        ("stddev by (job) (rate(reqs_total[3m]))", 180_000),
        (r#"count_values("v", rate(reqs_total[3m]))"#, 180_000),
        // Nested: a parameterized aggregation over a SUBQUERY-range inner,
        // which now routes through the planner (subquery sub-grid evaluated
        // per-step through the recursive planner, shared outer fold).
        ("quantile(0.5, max_over_time((m)[5m:1m]))", 120_000),
        // Unary-negation subquery inner: `Expr::Unary` now routes through the
        // planner, so the subquery's structural gate accepts it.
        ("quantile(0.5, max_over_time((-m)[5m:1m]))", 120_000),
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
    }

    // The experimental `limitk`/`limit_ratio` param aggregations now route
    // through the planner via the shared interpreter kernels (incl.
    // `limit_ratio`'s `InvalidRatioWarning`); their parity is checked in
    // `experimental_param_aggregate_planner_path_matches_interpreter`.
}
