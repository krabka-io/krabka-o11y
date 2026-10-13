use super::*;

struct AggregateMemberCase {
    job: &'static str,
    instance: &'static str,
    value: f64,
}

struct SingleMemberCase {
    job: &'static str,
    value: f64,
}

struct RepeatedValueCase {
    instance: &'static str,
    value: f64,
}

struct CounterPathCase {
    path: &'static str,
    factor: f64,
}

struct ParameterizedQueryCase {
    query: &'static str,
    time_ms: i64,
}

#[tokio::test]
pub(crate) async fn param_aggregate_planner_path_matches_interpreter() {
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
    for case in [
        AggregateMemberCase {
            job: "api",
            instance: "0",
            value: 5.0,
        },
        AggregateMemberCase {
            job: "api",
            instance: "1",
            value: 5.0,
        }, // ties with api/0 under topk/bottomk
        AggregateMemberCase {
            job: "api",
            instance: "2",
            value: 2.0,
        },
        AggregateMemberCase {
            job: "api",
            instance: "3",
            value: 8.0,
        },
        AggregateMemberCase {
            job: "api",
            instance: "4",
            value: f64::NAN,
        }, // genuine NaN member
        AggregateMemberCase {
            job: "db",
            instance: "0",
            value: 1.0,
        },
        AggregateMemberCase {
            job: "db",
            instance: "1",
            value: 9.0,
        },
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "m"),
            crate::test_support::TestLabelPair::new("job", case.job),
            crate::test_support::TestLabelPair::new("instance", case.instance),
        ]);
        store.push_float("t", lbls, 120_000, case.value);
    }
    // A single-member group per job (single-element quantile/stddev/stdvar).
    for case in [
        SingleMemberCase {
            job: "api",
            value: 4.0,
        },
        SingleMemberCase {
            job: "db",
            value: 7.0,
        },
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "single"),
            crate::test_support::TestLabelPair::new("job", case.job),
        ]);
        store.push_float("t", lbls, 120_000, case.value);
    }
    // Repeated values for count_values: 1, 1, 2 within job=api.
    for case in [
        RepeatedValueCase {
            instance: "0",
            value: 1.0,
        },
        RepeatedValueCase {
            instance: "1",
            value: 1.0,
        },
        RepeatedValueCase {
            instance: "2",
            value: 2.0,
        },
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "cv"),
            crate::test_support::TestLabelPair::new("job", "api"),
            crate::test_support::TestLabelPair::new("instance", case.instance),
        ]);
        store.push_float("t", lbls, 120_000, case.value);
    }
    // Counters for `topk(.., rate(...))` (slope = factor / 60s).
    for case in [
        CounterPathCase {
            path: "a",
            factor: 1.0,
        },
        CounterPathCase {
            path: "b",
            factor: 2.0,
        },
        CounterPathCase {
            path: "c",
            factor: 5.0,
        },
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "reqs_total"),
            crate::test_support::TestLabelPair::new("job", "api"),
            crate::test_support::TestLabelPair::new("path", case.path),
        ]);
        push_float_counter_series(&mut store, lbls, case.factor, 3);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Each query must route through the operator path and match the
    // interpreter byte-for-byte (NaN-aware, bit-exact).
    let queries = [
        // topk/bottomk: original series kept (labels incl. __name__), tie-break
        // by labels_key, k clamping (k > group size, k = 0). by and without.
        ParameterizedQueryCase {
            query: "topk(2, m)",
            time_ms: 120_000_i64,
        },
        ParameterizedQueryCase {
            query: "bottomk(2, m)",
            time_ms: 120_000,
        },
        ParameterizedQueryCase {
            query: "topk(2, m) by (job)",
            time_ms: 120_000,
        },
        ParameterizedQueryCase {
            query: "bottomk(2, m) by (job)",
            time_ms: 120_000,
        },
        ParameterizedQueryCase {
            query: "topk(2, m) without (instance)",
            time_ms: 120_000,
        },
        // k larger than a group's size clamps to the whole group.
        ParameterizedQueryCase {
            query: "topk(10, m) by (job)",
            time_ms: 120_000,
        },
        // k = 0 yields the empty vector.
        ParameterizedQueryCase {
            query: "topk(0, m)",
            time_ms: 120_000,
        },
        ParameterizedQueryCase {
            query: "bottomk(0, m) by (job)",
            time_ms: 120_000,
        },
        // Ties across the whole vector: api/0 and api/1 both 5.0.
        ParameterizedQueryCase {
            query: "topk(3, m)",
            time_ms: 120_000,
        },
        // quantile: phi = 0 / 0.5 / 0.9 / 1, by and without.
        ParameterizedQueryCase {
            query: "quantile(0, m) by (job)",
            time_ms: 120_000,
        },
        ParameterizedQueryCase {
            query: "quantile(0.5, m) by (job)",
            time_ms: 120_000,
        },
        ParameterizedQueryCase {
            query: "quantile(0.9, m) by (job)",
            time_ms: 120_000,
        },
        ParameterizedQueryCase {
            query: "quantile(1, m) by (job)",
            time_ms: 120_000,
        },
        ParameterizedQueryCase {
            query: "quantile(0.5, m) without (instance)",
            time_ms: 120_000,
        },
        // Single-element group: quantile equals the lone value.
        ParameterizedQueryCase {
            query: "quantile(0.5, single) by (job)",
            time_ms: 120_000,
        },
        // count_values: one series per distinct value, value -> label, count.
        ParameterizedQueryCase {
            query: r#"count_values("v", cv)"#,
            time_ms: 120_000,
        },
        ParameterizedQueryCase {
            query: r#"count_values("v", cv) by (job)"#,
            time_ms: 120_000,
        },
        ParameterizedQueryCase {
            query: r#"count_values("v", cv) without (instance)"#,
            time_ms: 120_000,
        },
        // stddev/stdvar: population std-dev / variance, by and without.
        ParameterizedQueryCase {
            query: "stddev(m) by (job)",
            time_ms: 120_000,
        },
        ParameterizedQueryCase {
            query: "stdvar(m) by (job)",
            time_ms: 120_000,
        },
        ParameterizedQueryCase {
            query: "stddev(m) without (instance)",
            time_ms: 120_000,
        },
        ParameterizedQueryCase {
            query: "stdvar without (instance) (m)",
            time_ms: 120_000,
        },
        // Single-element group -> stddev/stdvar = 0.
        ParameterizedQueryCase {
            query: "stddev(single) by (job)",
            time_ms: 120_000,
        },
        ParameterizedQueryCase {
            query: "stdvar(single) by (job)",
            time_ms: 120_000,
        },
        // No modifier (collapse all).
        ParameterizedQueryCase {
            query: "stddev(m)",
            time_ms: 120_000,
        },
        ParameterizedQueryCase {
            query: "quantile(0.5, m)",
            time_ms: 120_000,
        },
        // Nested: a parameterized aggregation over a rate inner already on the
        // operator path.
        ParameterizedQueryCase {
            query: "topk(1, rate(reqs_total[3m]))",
            time_ms: 180_000,
        },
        ParameterizedQueryCase {
            query: "quantile(0.5, rate(reqs_total[3m]))",
            time_ms: 180_000,
        },
        ParameterizedQueryCase {
            query: "stddev by (job) (rate(reqs_total[3m]))",
            time_ms: 180_000,
        },
        ParameterizedQueryCase {
            query: r#"count_values("v", rate(reqs_total[3m]))"#,
            time_ms: 180_000,
        },
        // Nested: a parameterized aggregation over a SUBQUERY-range inner,
        // which now routes through the planner (subquery sub-grid evaluated
        // per-step through the recursive planner, shared outer fold).
        ParameterizedQueryCase {
            query: "quantile(0.5, max_over_time((m)[5m:1m]))",
            time_ms: 120_000,
        },
        // Unary-negation subquery inner: `Expr::Unary` now routes through the
        // planner, so the subquery's structural gate accepts it.
        ParameterizedQueryCase {
            query: "quantile(0.5, max_over_time((-m)[5m:1m]))",
            time_ms: 120_000,
        },
    ];

    for case in queries {
        let query = case.query;
        let time_ms = case.time_ms;
        let SortedParity {
            via_operators,
            via_interpreter,
        } = sorted_planned_and_interpreted(&engine, query, time_ms).await;
        assert2::assert!(instant_samples_match(&via_interpreter, &via_operators));
    }

    // The experimental `limitk`/`limit_ratio` param aggregations now route
    // through the planner via the shared interpreter kernels (incl.
    // `limit_ratio`'s `InvalidRatioWarning`); their parity is checked in
    // `experimental_param_aggregate_planner_path_matches_interpreter`.
}
