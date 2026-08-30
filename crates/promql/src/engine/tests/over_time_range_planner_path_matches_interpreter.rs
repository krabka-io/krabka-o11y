use super::*;

#[tokio::test]
pub(crate) async fn over_time_range_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A float-only store: a multi-sample gauge for the reductions, a second
    // labelset, a single-sample window edge case, and a stale marker that the
    // matrix path drops.
    let mut store = InMemoryMetricStore::new();
    let stale_bits = f64::from_bits(0x7ff0_0000_0000_0002);
    for (lbls, samples) in [
        (
            labels(&[("__name__", "queue_depth"), ("job", "api")]),
            vec![
                (60_000_i64, 2.0),
                (120_000, 4.0),
                (180_000, 4.0),
                (240_000, 5.0),
                (300_000, 9.0),
            ],
        ),
        (
            labels(&[("__name__", "queue_depth"), ("job", "db")]),
            vec![(120_000, 3.0), (240_000, 7.0), (300_000, 1.0)],
        ),
        (
            // A stale marker mid-window: dropped by both paths.
            labels(&[("__name__", "queue_depth"), ("job", "stale")]),
            vec![(120_000, 5.0), (180_000, stale_bits), (300_000, 6.0)],
        ),
        (
            // Single in-window sample: rate yields no value, but over_time
            // reductions (avg/min/max/last/...) do.
            labels(&[("__name__", "queue_depth"), ("job", "lonely")]),
            vec![(295_000, 100.0)],
        ),
        // A `g`-grouped family for the SPARSE aggregate-over-over_time case at a
        // TIGHT `[30s]` window closing on t=300000 (window (270k, 300k]):
        //   g="mix": a member WITH an in-window sample (300k) -> has a value,
        //     plus a member whose only sample (120k) is outside the window ->
        //     no value (NULL). The no-value member is excluded, so the group
        //     survives with only the in-window member.
        //   g="allsparse": every member's only sample is outside the window,
        //     so the whole group is no-value and produces NO result row.
        (
            labels(&[("__name__", "depth_g"), ("g", "mix"), ("instance", "0")]),
            vec![(300_000, 5.0)],
        ),
        (
            labels(&[("__name__", "depth_g"), ("g", "mix"), ("instance", "1")]),
            vec![(120_000, 9.0)],
        ),
        (
            labels(&[
                ("__name__", "depth_g"),
                ("g", "allsparse"),
                ("instance", "0"),
            ]),
            vec![(120_000, 1.0)],
        ),
        (
            labels(&[
                ("__name__", "depth_g"),
                ("g", "allsparse"),
                ("instance", "1"),
            ]),
            vec![(120_000, 2.0)],
        ),
    ] {
        for (ts, value) in samples {
            store.push_float("t", lbls.clone(), ts, value);
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries = [
        ("avg_over_time(queue_depth[5m])", 300_000_i64),
        ("sum_over_time(queue_depth[5m])", 300_000),
        ("count_over_time(queue_depth[5m])", 300_000),
        ("min_over_time(queue_depth[5m])", 300_000),
        ("max_over_time(queue_depth[5m])", 300_000),
        ("stddev_over_time(queue_depth[5m])", 300_000),
        ("stdvar_over_time(queue_depth[5m])", 300_000),
        // `last_over_time` preserves the metric name; every other family drops it.
        ("last_over_time(queue_depth[5m])", 300_000),
        ("present_over_time(queue_depth[5m])", 300_000),
        ("quantile_over_time(0.5, queue_depth[5m])", 300_000),
        ("quantile_over_time(0.9, queue_depth[5m])", 300_000),
        // @ and offset on the matrix selector exercise the time modifier.
        ("avg_over_time(queue_depth[3m] @ 300)", 999_999),
        ("sum_over_time(queue_depth[4m] offset 1m)", 360_000),
        // Tighter window that strands the single-sample series for some fns.
        ("min_over_time(queue_depth[90s])", 300_000),
        // EXPERIMENTAL over_time members now route through the shared-kernel
        // operator path. `first_over_time` preserves `__name__`; the `ts_of_*`
        // family returns the matching sample's timestamp in seconds.
        ("mad_over_time(queue_depth[5m])", 300_000),
        ("first_over_time(queue_depth[5m])", 300_000),
        ("ts_of_min_over_time(queue_depth[5m])", 300_000),
        ("ts_of_max_over_time(queue_depth[5m])", 300_000),
        ("ts_of_first_over_time(queue_depth[5m])", 300_000),
        ("ts_of_last_over_time(queue_depth[5m])", 300_000),
        // Experimental members composed under an aggregation also route.
        ("sum by (job) (mad_over_time(queue_depth[5m]))", 300_000),
        (
            "count by (job) (ts_of_max_over_time(queue_depth[5m]))",
            300_000,
        ),
        // @ / offset on an experimental member.
        ("first_over_time(queue_depth[3m] @ 300)", 999_999),
        // Aggregation over over_time: the compositional operator-path case.
        ("sum by (job) (avg_over_time(queue_depth[5m]))", 300_000),
        (
            "max without (job) (last_over_time(queue_depth[5m]))",
            300_000,
        ),
        // SPARSE aggregate-over-over_time: a group mixing an in-window member
        // with a no-value (stranded) member excludes the no-value member, and
        // an all-no-value group produces no result row. Every op must agree
        // with the interpreter.
        ("sum by (g) (avg_over_time(depth_g[30s]))", 300_000),
        ("count by (g) (avg_over_time(depth_g[30s]))", 300_000),
        ("min by (g) (max_over_time(depth_g[30s]))", 300_000),
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
        // NaN-aware comparison (a genuine NaN reduction equals itself).
        assert2::assert!(instant_samples_match(&via_interpreter, &via_operators));

        // Pin the SPARSE aggregate-over-over_time rule: the no-value (stranded)
        // member is excluded from its group, and the all-no-value group is
        // absent.
        if matches!(
            query,
            "sum by (g) (avg_over_time(depth_g[30s]))"
                | "count by (g) (avg_over_time(depth_g[30s]))"
                | "min by (g) (max_over_time(depth_g[30s]))"
        ) {
            assert2::assert!(via_operators.len() == 1);
            let mix = via_operators
                .iter()
                .find(|sample| sample.labels.get("g") == Some("mix"));
            assert2::assert!(mix.is_some());
            assert2::assert!(
                via_operators
                    .iter()
                    .all(|sample| sample.labels.get("g") != Some("allsparse"))
            );
            if query == "count by (g) (avg_over_time(depth_g[30s]))" {
                assert2::assert!(approx_eq(float_value(&mix.unwrap().value), 1.0));
            }
        }
    }

    // The experimental over_time members (`mad`/`first`/`ts_of_*`) now route
    // through the shared-kernel operator path and are differentially checked in
    // the `queries` list above; pin that they are in fact claimed by the planner.
    for query in [
        "mad_over_time(queue_depth[5m])",
        "first_over_time(queue_depth[5m])",
        "ts_of_min_over_time(queue_depth[5m])",
        "ts_of_max_over_time(queue_depth[5m])",
        "ts_of_first_over_time(queue_depth[5m])",
        "ts_of_last_over_time(queue_depth[5m])",
    ] {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(300_000))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let planned = engine
            .plan_instant_expr("t", &expr, 300_000)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"));
        assert2::assert!(planned.is_some());
    }
}
