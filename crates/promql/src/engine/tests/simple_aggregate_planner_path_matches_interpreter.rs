use super::*;

struct AggregateInput {
    job: &'static str,
    group: &'static str,
    instance: &'static str,
    value: f64,
}

struct TimestampedInput {
    instance: &'static str,
    timestamp_ms: i64,
    value: f64,
}

struct MinMaxInput {
    group: &'static str,
    instance: &'static str,
    value: f64,
}

struct CounterInput {
    job: &'static str,
    path: &'static str,
    factor: f64,
}

struct GroupInput {
    group: &'static str,
    instance: &'static str,
}

type AggregateQueryCase = QueryAt;

#[tokio::test]
pub(crate) async fn simple_aggregate_planner_path_matches_interpreter() {
    // A float-only multi-label store: two jobs across two groups, an
    // instance dimension for `without`, plus counters for the rate case.
    let mut store = InMemoryMetricStore::new();
    for input in [
        AggregateInput {
            job: "api",
            group: "prod",
            instance: "0",
            value: 1.0,
        },
        AggregateInput {
            job: "api",
            group: "prod",
            instance: "1",
            value: 2.0,
        },
        AggregateInput {
            job: "api",
            group: "canary",
            instance: "0",
            value: 4.0,
        },
        AggregateInput {
            job: "db",
            group: "prod",
            instance: "0",
            value: 8.0,
        },
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "http_requests"),
            crate::test_support::TestLabelPair::new("job", input.job),
            crate::test_support::TestLabelPair::new("group", input.group),
            crate::test_support::TestLabelPair::new("instance", input.instance),
        ]);
        store.push_float("t", lbls, 120_000, input.value);
    }
    // A dedicated NaN metric exercising the post-fix selection semantics
    // through `sum`/`count`. instance=0 is a finite value, instance=1's
    // latest in-window sample is a GENUINE NaN (must be KEPT and flow into
    // the aggregate, so `sum(nan_metric)` is NaN and `count(nan_metric)` is
    // 2), and instance=2's latest in-window sample is a STALE-NaN marker
    // (must be DROPPED before aggregation, so it does not contribute to
    // `count`). Both paths must agree.
    for input in [
        TimestampedInput {
            instance: "0",
            timestamp_ms: 120_000_i64,
            value: 3.0,
        },
        TimestampedInput {
            instance: "1",
            timestamp_ms: 60_000,
            value: 5.0,
        },
        TimestampedInput {
            instance: "1",
            timestamp_ms: 120_000,
            value: f64::NAN,
        },
        TimestampedInput {
            instance: "2",
            timestamp_ms: 60_000,
            value: 9.0,
        },
        TimestampedInput {
            instance: "2",
            timestamp_ms: 120_000,
            value: stale_nan(),
        },
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "nan_metric"),
            crate::test_support::TestLabelPair::new("job", "api"),
            crate::test_support::TestLabelPair::new("instance", input.instance),
        ]);
        store.push_float("t", lbls, input.timestamp_ms, input.value);
    }
    // A dedicated metric pinning the `min`/`max` NaN-ignoring rule. Group
    // g="mixed" holds genuine NaN alongside finite samples: Prometheus (and
    // the interpreter) take the extremum over the non-NaN values (NaN
    // ignored), so min=1, max=4. Group g="allnan" is entirely NaN:
    // Prometheus keeps the series with a NaN result (it is not dropped).
    // Arrow's built-in min/max instead order floats with total_cmp and
    // PROPAGATE NaN, so the operator path must use the NaN-ignoring UDAFs to
    // match the interpreter here.
    for input in [
        MinMaxInput {
            group: "mixed",
            instance: "0",
            value: f64::NAN,
        },
        MinMaxInput {
            group: "mixed",
            instance: "1",
            value: 4.0,
        },
        MinMaxInput {
            group: "mixed",
            instance: "2",
            value: 1.0,
        },
        MinMaxInput {
            group: "mixed",
            instance: "3",
            value: f64::NAN,
        },
        MinMaxInput {
            group: "allnan",
            instance: "0",
            value: f64::NAN,
        },
        MinMaxInput {
            group: "allnan",
            instance: "1",
            value: f64::NAN,
        },
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "minmax_nan"),
            crate::test_support::TestLabelPair::new("g", input.group),
            crate::test_support::TestLabelPair::new("instance", input.instance),
        ]);
        store.push_float("t", lbls, 120_000, input.value);
    }
    // Counters for `sum by (...) (rate(...))` (slope = step factor / 60s).
    for input in [
        CounterInput {
            job: "api",
            path: "a",
            factor: 1.0,
        },
        CounterInput {
            job: "api",
            path: "b",
            factor: 2.0,
        },
        CounterInput {
            job: "db",
            path: "a",
            factor: 5.0,
        },
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "reqs_total"),
            crate::test_support::TestLabelPair::new("job", input.job),
            crate::test_support::TestLabelPair::new("path", input.path),
        ]);
        for step in 0..=3_i32 {
            store.push_float(
                "t",
                lbls.clone(),
                i64::from(step) * 60_000,
                f64::from(step) * input.factor,
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
    for input in [
        GroupInput {
            group: "mix",
            instance: "dense",
        },
        GroupInput {
            group: "mix",
            instance: "sparse",
        },
        GroupInput {
            group: "allsparse",
            instance: "0",
        },
        GroupInput {
            group: "allsparse",
            instance: "1",
        },
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "sparse_total"),
            crate::test_support::TestLabelPair::new("g", input.group),
            crate::test_support::TestLabelPair::new("instance", input.instance),
        ]);
        if input.instance == "dense" {
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
        AggregateQueryCase {
            query: "sum by (group) (http_requests)",
            time_ms: 120_000_i64,
        },
        AggregateQueryCase {
            query: "avg by (group) (http_requests)",
            time_ms: 120_000,
        },
        AggregateQueryCase {
            query: "min by (group) (http_requests)",
            time_ms: 120_000,
        },
        AggregateQueryCase {
            query: "max by (group) (http_requests)",
            time_ms: 120_000,
        },
        AggregateQueryCase {
            query: "count by (group) (http_requests)",
            time_ms: 120_000,
        },
        AggregateQueryCase {
            query: "group by (group) (http_requests)",
            time_ms: 120_000,
        },
        AggregateQueryCase {
            query: "sum without (instance) (http_requests)",
            time_ms: 120_000,
        },
        AggregateQueryCase {
            query: "sum without () (http_requests)",
            time_ms: 120_000,
        },
        AggregateQueryCase {
            query: "sum by () (http_requests)",
            time_ms: 120_000,
        },
        AggregateQueryCase {
            query: "sum(http_requests)",
            time_ms: 120_000,
        },
        AggregateQueryCase {
            query: "sum by (job, nonexistent) (http_requests)",
            time_ms: 120_000,
        },
        AggregateQueryCase {
            query: "sum(((http_requests)))",
            time_ms: 120_000,
        },
        // Empty-input aggregations must yield an empty vector (no global
        // group row), matching Prometheus and the interpreter.
        AggregateQueryCase {
            query: "sum by () (does_not_exist)",
            time_ms: 120_000,
        },
        AggregateQueryCase {
            query: "sum(does_not_exist)",
            time_ms: 120_000,
        },
        AggregateQueryCase {
            query: "count by (group) (does_not_exist)",
            time_ms: 120_000,
        },
        // Aggregation over a rate call: the marquee operator-path case.
        // `sum by (l) (rate(x[range]))` mirrors the diff-corpus query
        // `sum by (method) (rate(http_requests_total[30s]))`.
        AggregateQueryCase {
            query: "sum by (job) (rate(reqs_total[3m]))",
            time_ms: 180_000,
        },
        AggregateQueryCase {
            query: "sum by (path) (rate(reqs_total[90s]))",
            time_ms: 180_000,
        },
        AggregateQueryCase {
            query: "max without (path) (rate(reqs_total[3m]))",
            time_ms: 180_000,
        },
        // SPARSE aggregate-over-rate (the headline divergence the fix closes):
        // a group mixing a dense rate with a no-value (sparse) rate must
        // exclude the no-value series, and an all-no-value group must produce
        // no result row. Every simple op must agree with the interpreter.
        AggregateQueryCase {
            query: "sum by (g) (rate(sparse_total[2m]))",
            time_ms: 180_000,
        },
        AggregateQueryCase {
            query: "avg by (g) (rate(sparse_total[2m]))",
            time_ms: 180_000,
        },
        AggregateQueryCase {
            query: "min by (g) (rate(sparse_total[2m]))",
            time_ms: 180_000,
        },
        AggregateQueryCase {
            query: "max by (g) (rate(sparse_total[2m]))",
            time_ms: 180_000,
        },
        AggregateQueryCase {
            query: "count by (g) (rate(sparse_total[2m]))",
            time_ms: 180_000,
        },
        AggregateQueryCase {
            query: "group by (g) (rate(sparse_total[2m]))",
            time_ms: 180_000,
        },
        // No grouping: the global aggregate is over the single dense rate
        // (every sparse series is no-value and excluded). One result row.
        AggregateQueryCase {
            query: "sum (rate(sparse_total[2m]))",
            time_ms: 180_000,
        },
        AggregateQueryCase {
            query: "count (rate(sparse_total[2m]))",
            time_ms: 180_000,
        },
        // The same fix on `*_over_time`: avg_over_time has a value for the
        // single-sample sparse members too, but a TIGHT window can strand
        // them. Use a window narrow enough that the sparse members fall
        // outside it at t=180000 while the dense member still reduces.
        AggregateQueryCase {
            query: "count by (g) (avg_over_time(sparse_total[30s]))",
            time_ms: 180_000,
        },
        // Genuine NaN flows into the aggregate (sum -> NaN), and the
        // stale-NaN marker is dropped before counting (count -> 2).
        AggregateQueryCase {
            query: "sum(nan_metric)",
            time_ms: 120_000,
        },
        AggregateQueryCase {
            query: "count(nan_metric)",
            time_ms: 120_000,
        },
        // NaN-ignoring min/max: the "mixed" group's extremum is over its
        // non-NaN samples (min=1, max=4); the "allnan" group keeps the
        // series with a NaN result. The operator path (NaN-ignoring UDAFs)
        // must match the interpreter bit-for-bit on every group, including
        // the all-NaN -> NaN case (a plain `value != value` filter would
        // instead drop the all-NaN series).
        AggregateQueryCase {
            query: "min by (g) (minmax_nan)",
            time_ms: 120_000,
        },
        AggregateQueryCase {
            query: "max by (g) (minmax_nan)",
            time_ms: 120_000,
        },
        AggregateQueryCase {
            query: "min(minmax_nan)",
            time_ms: 120_000,
        },
        AggregateQueryCase {
            query: "max(minmax_nan)",
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
