use super::*;

#[tokio::test]
pub(crate) async fn simple_aggregate_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A float-only multi-label store: two jobs across two groups, an
    // instance dimension for `without`, plus counters for the rate case.
    let mut store = InMemoryMetricStore::new();
    for (job, group, instance, value) in [
        ("api", "prod", "0", 1.0),
        ("api", "prod", "1", 2.0),
        ("api", "canary", "0", 4.0),
        ("db", "prod", "0", 8.0),
    ] {
        let lbls = labels(&[
            ("__name__", "http_requests"),
            ("job", job),
            ("group", group),
            ("instance", instance),
        ]);
        store.push_float("t", lbls, 120_000, value);
    }
    // A dedicated NaN metric exercising the post-fix selection semantics
    // through `sum`/`count`. instance=0 is a finite value, instance=1's
    // latest in-window sample is a GENUINE NaN (must be KEPT and flow into
    // the aggregate, so `sum(nan_metric)` is NaN and `count(nan_metric)` is
    // 2), and instance=2's latest in-window sample is a STALE-NaN marker
    // (must be DROPPED before aggregation, so it does not contribute to
    // `count`). Both paths must agree.
    for (instance, ts, value) in [
        ("0", 120_000_i64, 3.0),
        ("1", 60_000, 5.0),
        ("1", 120_000, f64::NAN),
        ("2", 60_000, 9.0),
        ("2", 120_000, stale_nan()),
    ] {
        let lbls = labels(&[
            ("__name__", "nan_metric"),
            ("job", "api"),
            ("instance", instance),
        ]);
        store.push_float("t", lbls, ts, value);
    }
    // A dedicated metric pinning the `min`/`max` NaN-ignoring rule. Group
    // g="mixed" holds genuine NaN alongside finite samples: Prometheus (and
    // the interpreter) take the extremum over the non-NaN values (NaN
    // ignored), so min=1, max=4. Group g="allnan" is entirely NaN:
    // Prometheus keeps the series with a NaN result (it is not dropped).
    // Arrow's built-in min/max instead order floats with total_cmp and
    // PROPAGATE NaN, so the operator path must use the NaN-ignoring UDAFs to
    // match the interpreter here.
    for (group, instance, value) in [
        ("mixed", "0", f64::NAN),
        ("mixed", "1", 4.0),
        ("mixed", "2", 1.0),
        ("mixed", "3", f64::NAN),
        ("allnan", "0", f64::NAN),
        ("allnan", "1", f64::NAN),
    ] {
        let lbls = labels(&[
            ("__name__", "minmax_nan"),
            ("g", group),
            ("instance", instance),
        ]);
        store.push_float("t", lbls, 120_000, value);
    }
    // Counters for `sum by (...) (rate(...))` (slope = step factor / 60s).
    for (job, path, factor) in [("api", "a", 1.0), ("api", "b", 2.0), ("db", "a", 5.0)] {
        let lbls = labels(&[("__name__", "reqs_total"), ("job", job), ("path", path)]);
        for step in 0..=3_i32 {
            store.push_float(
                "t",
                lbls.clone(),
                i64::from(step) * 60_000,
                f64::from(step) * factor,
            );
        }
    }
    // Counters for the SPARSE aggregate-over-rate parity. The `g` label groups
    // members; at the 2m rate window closing on t=180_000:
    //   g="mix": one DENSE member (full history -> rate has a value) plus one
    //     SPARSE member (a single in-window sample -> rate is no-value). The
    //     no-value series must be excluded, so `sum by(g)(rate)` over g="mix"
    //     equals just the dense member's rate and `count by(g)(rate)` is 1.
    //   g="allsparse": every member is a single-sample (no-value) series, so
    //     the whole group collapses to NO result row (series absent), matching
    //     the interpreter, which forms no group when no sample reaches it.
    for (g, instance) in [
        ("mix", "dense"),
        ("mix", "sparse"),
        ("allsparse", "0"),
        ("allsparse", "1"),
    ] {
        let lbls = labels(&[
            ("__name__", "sparse_total"),
            ("g", g),
            ("instance", instance),
        ]);
        if instance == "dense" {
            // A full counter history: rate has a value at t=180_000.
            for step in 0..=3_i32 {
                store.push_float(
                    "t",
                    lbls.clone(),
                    i64::from(step) * 60_000,
                    f64::from(step) * 7.0,
                );
            }
        } else {
            // A single in-window sample: rate yields no value (NULL).
            store.push_float("t", lbls.clone(), 120_000, 100.0);
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries = [
        ("sum by (group) (http_requests)", 120_000_i64),
        ("avg by (group) (http_requests)", 120_000),
        ("min by (group) (http_requests)", 120_000),
        ("max by (group) (http_requests)", 120_000),
        ("count by (group) (http_requests)", 120_000),
        ("group by (group) (http_requests)", 120_000),
        ("sum without (instance) (http_requests)", 120_000),
        ("sum without () (http_requests)", 120_000),
        ("sum by () (http_requests)", 120_000),
        ("sum(http_requests)", 120_000),
        ("sum by (job, nonexistent) (http_requests)", 120_000),
        ("sum(((http_requests)))", 120_000),
        // Empty-input aggregations must yield an empty vector (no global
        // group row), matching Prometheus and the interpreter.
        ("sum by () (does_not_exist)", 120_000),
        ("sum(does_not_exist)", 120_000),
        ("count by (group) (does_not_exist)", 120_000),
        // Aggregation over a rate call: the marquee operator-path case.
        // `sum by (l) (rate(x[range]))` mirrors the diff-corpus query
        // `sum by (method) (rate(http_requests_total[30s]))`.
        ("sum by (job) (rate(reqs_total[3m]))", 180_000),
        ("sum by (path) (rate(reqs_total[90s]))", 180_000),
        ("max without (path) (rate(reqs_total[3m]))", 180_000),
        // SPARSE aggregate-over-rate (the headline divergence the fix closes):
        // a group mixing a dense rate with a no-value (sparse) rate must
        // exclude the no-value series, and an all-no-value group must produce
        // no result row. Every simple op must agree with the interpreter.
        ("sum by (g) (rate(sparse_total[2m]))", 180_000),
        ("avg by (g) (rate(sparse_total[2m]))", 180_000),
        ("min by (g) (rate(sparse_total[2m]))", 180_000),
        ("max by (g) (rate(sparse_total[2m]))", 180_000),
        ("count by (g) (rate(sparse_total[2m]))", 180_000),
        ("group by (g) (rate(sparse_total[2m]))", 180_000),
        // No grouping: the global aggregate is over the single dense rate
        // (every sparse series is no-value and excluded). One result row.
        ("sum (rate(sparse_total[2m]))", 180_000),
        ("count (rate(sparse_total[2m]))", 180_000),
        // The same fix on `*_over_time`: avg_over_time has a value for the
        // single-sample sparse members too, but a TIGHT window can strand
        // them. Use a window narrow enough that the sparse members fall
        // outside it at t=180000 while the dense member still reduces.
        ("count by (g) (avg_over_time(sparse_total[30s]))", 180_000),
        // Genuine NaN flows into the aggregate (sum -> NaN), and the
        // stale-NaN marker is dropped before counting (count -> 2).
        ("sum(nan_metric)", 120_000),
        ("count(nan_metric)", 120_000),
        // NaN-ignoring min/max: the "mixed" group's extremum is over its
        // non-NaN samples (min=1, max=4); the "allnan" group keeps the
        // series with a NaN result. The operator path (NaN-ignoring UDAFs)
        // must match the interpreter bit-for-bit on every group, including
        // the all-NaN -> NaN case (a plain `value != value` filter would
        // instead drop the all-NaN series).
        ("min by (g) (minmax_nan)", 120_000),
        ("max by (g) (minmax_nan)", 120_000),
        ("min(minmax_nan)", 120_000),
        ("max(minmax_nan)", 120_000),
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

        // Pin the staleness semantics through the aggregate: the genuine NaN
        // in `nan_metric` is kept (so `sum(nan_metric)` is NaN), and the
        // stale-NaN marker is dropped before counting (so `count(nan_metric)`
        // is 2, not 3).
        assert_aggregate_nan_staleness(query, &via_operators);
        // Pin the NaN-ignoring min/max rule on absolute values (not just
        // operator==interpreter): the mixed group's extremum is over its
        // non-NaN samples, and the all-NaN group is kept with a NaN result.
        assert_minmax_nan_ignoring(query, &via_operators);
        // Pin the SPARSE aggregate-over-rate rule on absolute values: the
        // no-value (sparse) series is excluded from its group, and an
        // all-no-value group produces no result row at all.
        assert_sparse_aggregate_excludes_no_value(query, &via_operators);
    }
}
