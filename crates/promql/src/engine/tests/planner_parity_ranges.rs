use super::*;

mod classic_histogram_quantile_planner_path_matches_interpreter {
use super::*;

struct ClassicBucketCase {
    job: &'static str,
    boundary: &'static str,
    count: f64,
}

struct CounterBucketCase {
    boundary: &'static str,
    factor: f64,
}

struct QuantileQueryCase {
    query: &'static str,
    time_ms: i64,
    retains_name_until_boundary: bool,
}

#[tokio::test]
pub(crate) async fn classic_histogram_quantile_planner_path_matches_interpreter() {
    // A float-only store of classic `<metric>_bucket{le}` series exercising the
    // classic histogram_quantile fold:
    //  - `lat_bucket{job}`: a well-formed monotonic histogram with a real `+Inf`
    //    overflow bucket, in two groups (job=api / job=db) so the multi-group
    //    case and the `__name__` + `le` drop are both observable.
    //  - `nonmono_bucket`: a NON-monotonic cumulative bucket set (the le=2 count
    //    dips below le=1) so the monotonicity-forcing path is taken.
    //  - `inf_only_bucket`: a single `+Inf` bucket (<2 buckets -> NaN).
    //  - `reqs_bucket{le}`: counters for the NESTED
    //    `histogram_quantile(0.9, sum by (le) (rate(reqs_bucket[5m])))` case,
    //    whose fully-float inner plans through the rate + aggregate operators.
    let mut store = InMemoryMetricStore::new();
    for sample in [
        ClassicBucketCase {
            job: "api",
            boundary: "0.1",
            count: 1.0,
        },
        ClassicBucketCase {
            job: "api",
            boundary: "0.2",
            count: 2.0,
        },
        ClassicBucketCase {
            job: "api",
            boundary: "0.4",
            count: 4.0,
        },
        ClassicBucketCase {
            job: "api",
            boundary: "+Inf",
            count: 5.0,
        },
        ClassicBucketCase {
            job: "db",
            boundary: "0.1",
            count: 0.0,
        },
        ClassicBucketCase {
            job: "db",
            boundary: "0.2",
            count: 1.0,
        },
        ClassicBucketCase {
            job: "db",
            boundary: "0.4",
            count: 3.0,
        },
        ClassicBucketCase {
            job: "db",
            boundary: "+Inf",
            count: 3.0,
        },
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "lat_bucket"),
            crate::test_support::TestLabelPair::new("job", sample.job),
            crate::test_support::TestLabelPair::new("le", sample.boundary),
        ]);
        store.push_float("t", lbls, 300_000, sample.count);
    }
    // A non-monotonic cumulative bucket set: le=2 (count 3) dips below le=1
    // (count 5); the fold must force monotonicity before interpolating.
    for sample in [
        CounterBucketCase {
            boundary: "1",
            factor: 5.0,
        },
        CounterBucketCase {
            boundary: "2",
            factor: 3.0,
        },
        CounterBucketCase {
            boundary: "+Inf",
            factor: 8.0,
        },
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "nonmono_bucket"),
            crate::test_support::TestLabelPair::new("le", sample.boundary),
        ]);
        store.push_float("t", lbls, 300_000, sample.factor);
    }
    // A single `+Inf` bucket: fewer than two buckets -> NaN.
    store.push_float(
        "t",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "inf_only_bucket"),
            crate::test_support::TestLabelPair::new("le", "+Inf"),
        ]),
        300_000,
        7.0,
    );
    // Counters for the nested `histogram_quantile(.., sum by (le) (rate(...)))`
    // case (slope = factor / 60s within the 5m window).
    for sample in [
        CounterBucketCase {
            boundary: "0.1",
            factor: 1.0,
        },
        CounterBucketCase {
            boundary: "0.2",
            factor: 2.0,
        },
        CounterBucketCase {
            boundary: "0.4",
            factor: 4.0,
        },
        CounterBucketCase {
            boundary: "+Inf",
            factor: 5.0,
        },
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "reqs_bucket"),
            crate::test_support::TestLabelPair::new("le", sample.boundary),
        ]);
        push_float_counter_series(&mut store, lbls, sample.factor, 5);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Each query must route through the operator path and match the
    // interpreter byte-for-byte (NaN-aware, bit-exact).
    let queries = [
        // Normal linear interpolation, multi-group (job=api / job=db), with the
        // `__name__` and `le` labels dropped from the output.
        QuantileQueryCase {
            query: "histogram_quantile(0.5, lat_bucket)",
            time_ms: 300_000,
            retains_name_until_boundary: true,
        },
        QuantileQueryCase {
            query: "histogram_quantile(0.9, lat_bucket)",
            time_ms: 300_000,
            retains_name_until_boundary: true,
        },
        // phi at the boundaries 0 and 1.
        QuantileQueryCase {
            query: "histogram_quantile(0, lat_bucket)",
            time_ms: 300_000,
            retains_name_until_boundary: true,
        },
        QuantileQueryCase {
            query: "histogram_quantile(1, lat_bucket)",
            time_ms: 300_000,
            retains_name_until_boundary: true,
        },
        // phi out of [0, 1]: -Inf below, +Inf above.
        QuantileQueryCase {
            query: "histogram_quantile(-0.5, lat_bucket)",
            time_ms: 300_000,
            retains_name_until_boundary: true,
        },
        QuantileQueryCase {
            query: "histogram_quantile(1.5, lat_bucket)",
            time_ms: 300_000,
            retains_name_until_boundary: true,
        },
        // A non-monotonic cumulative bucket set is forced monotonic first.
        QuantileQueryCase {
            query: "histogram_quantile(0.5, nonmono_bucket)",
            time_ms: 300_000,
            retains_name_until_boundary: true,
        },
        // A single `+Inf` bucket (<2 buckets) yields NaN.
        QuantileQueryCase {
            query: "histogram_quantile(0.5, inf_only_bucket)",
            time_ms: 300_000,
            retains_name_until_boundary: true,
        },
        // NESTED: a fully-float inner that plans through the rate + aggregate
        // operators, then the classic fold over the assembled bucket vector.
        QuantileQueryCase {
            query: "histogram_quantile(0.9, sum by (le) (rate(reqs_bucket[5m])))",
            time_ms: 300_000,
            retains_name_until_boundary: false,
        },
    ];

    for case in queries {
        let SortedParity {
            via_operators,
            via_interpreter,
        } = sorted_planned_and_interpreted(&engine, case.query, case.time_ms).await;
        assert2::assert!(instant_samples_match(&via_interpreter, &via_operators));

        // `le` is gone immediately; `__name__` remains pending until the public
        // query boundary.
        assert2::assert!(via_operators.iter().all(|sample| {
            sample.labels.get("__name__").is_some() == case.retains_name_until_boundary
                && sample.labels.get("le").is_none()
                && sample.drop_name
        }));
    }
    // The native-histogram flavor of these folds (bare selector, native
    // `histogram_quantile`, the native accessors) now routes through the
    // planner too — see `native_histogram_planner_path_matches_interpreter`.
}

}

mod extended_range_fold_planner_matches_interpreter {
use super::*;

type RangeFoldSample = FloatSampleInput;

#[derive(bon::Builder)]
struct RangeFoldSeries {
    job: &'static str,
    samples: Vec<RangeFoldSample>,
}

/// Differential parity for the residual range-vector folds.
///
/// The planner routes these folds through the shared interpreter dispatch
/// `plan_extended_range_fold_call`. They are `changes`, `resets`, and `deriv`
/// over a plain matrix selector with no operator-leaf UDF, and the `anchored`
/// and `smoothed` extended-selector forms of `rate`, `increase`, `delta`,
/// `changes`, and `resets`. Each fold must plan to `Some` and must match the
/// interpreter's `eval_instant_expr` byte-for-byte.
#[tokio::test]
pub(crate) async fn extended_range_fold_planner_matches_interpreter() {
    let mut store = InMemoryMetricStore::new();
    // A monotonic-ish counter with a reset, sampled every 30s through t=300000.
    push_float_series(
        &mut store,
        [
            RangeFoldSeries::builder()
                .job("a")
                .samples(vec![
                    RangeFoldSample::builder()
                        .timestamp_ms(0)
                        .value(0.0)
                        .build(),
                    RangeFoldSample::builder()
                        .timestamp_ms(30_000)
                        .value(5.0)
                        .build(),
                    RangeFoldSample::builder()
                        .timestamp_ms(60_000)
                        .value(10.0)
                        .build(),
                    RangeFoldSample::builder()
                        .timestamp_ms(90_000)
                        .value(4.0)
                        .build(), // reset
                    RangeFoldSample::builder()
                        .timestamp_ms(120_000)
                        .value(9.0)
                        .build(),
                    RangeFoldSample::builder()
                        .timestamp_ms(150_000)
                        .value(15.0)
                        .build(),
                    RangeFoldSample::builder()
                        .timestamp_ms(180_000)
                        .value(21.0)
                        .build(),
                    RangeFoldSample::builder()
                        .timestamp_ms(210_000)
                        .value(25.0)
                        .build(),
                    RangeFoldSample::builder()
                        .timestamp_ms(240_000)
                        .value(30.0)
                        .build(),
                    RangeFoldSample::builder()
                        .timestamp_ms(270_000)
                        .value(33.0)
                        .build(),
                    RangeFoldSample::builder()
                        .timestamp_ms(300_000)
                        .value(40.0)
                        .build(),
                ])
                .build(),
            RangeFoldSeries::builder()
                .job("b")
                .samples(vec![
                    RangeFoldSample::builder()
                        .timestamp_ms(0)
                        .value(100.0)
                        .build(),
                    RangeFoldSample::builder()
                        .timestamp_ms(60_000)
                        .value(90.0)
                        .build(),
                    RangeFoldSample::builder()
                        .timestamp_ms(120_000)
                        .value(80.0)
                        .build(),
                    RangeFoldSample::builder()
                        .timestamp_ms(180_000)
                        .value(70.0)
                        .build(),
                    RangeFoldSample::builder()
                        .timestamp_ms(240_000)
                        .value(60.0)
                        .build(),
                    RangeFoldSample::builder()
                        .timestamp_ms(300_000)
                        .value(50.0)
                        .build(),
                ])
                .build(),
        ]
        .into_iter()
        .map(|series| LabeledFloatSeries {
            labels: labels(&[
                crate::test_support::TestLabelPair::new("__name__", "ctr"),
                crate::test_support::TestLabelPair::new("job", series.job),
            ]),
            samples: series.samples,
        }),
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let time_ms = 300_000_i64;

    let queries = [
        // changes/resets/deriv over a plain matrix (no operator-leaf UDF).
        "changes(ctr[5m])",
        "resets(ctr[5m])",
        "deriv(ctr[5m])",
        "changes(ctr[2m])",
        "resets(ctr[2m])",
        // anchored/smoothed extended-selector folds.
        "rate(anchored(ctr[5m]))",
        "increase(anchored(ctr[5m]))",
        "delta(anchored(ctr[5m]))",
        "changes(anchored(ctr[5m]))",
        "resets(anchored(ctr[5m]))",
        "rate(smoothed(ctr[5m]))",
        "increase(smoothed(ctr[5m]))",
        "delta(smoothed(ctr[5m]))",
        // predict_linear over a plain matrix.
        "predict_linear(ctr[5m], 60)",
    ];

    for query in queries {
        let parity = planned_and_interpreted(&engine, query, time_ms).await;
        let via_operators = parity.via_operators;
        let via_interpreter = parity.via_interpreter;

        let normalize = |result: QueryResult| fingerprint_sorted(result, query);
        let via_operators = normalize(via_operators);
        let via_interpreter = normalize(via_interpreter);
        assert2::assert!(instant_samples_match(&via_operators, &via_interpreter));
    }
}

}

mod over_time_range_planner_path_matches_interpreter {
use super::*;

fn depth_g_series(
    group: &str,
    instance: &str,
    timestamp_ms: i64,
    value: f64,
) -> LabeledFloatSeries {
    LabeledFloatSeries {
        labels: labels(&[
            crate::test_support::TestLabelPair::new("__name__", "depth_g"),
            crate::test_support::TestLabelPair::new("g", group),
            crate::test_support::TestLabelPair::new("instance", instance),
        ]),
        samples: vec![FloatSampleInput {
            timestamp_ms,
            value,
        }],
    }
}

struct OverTimeQueryCase {
    query: &'static str,
    time_ms: i64,
}

impl OverTimeQueryCase {
    const fn new(query: &'static str, time_ms: i64) -> Self {
        Self { query, time_ms }
    }
}

#[tokio::test]
pub(crate) async fn over_time_range_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A float-only store: a multi-sample gauge for the reductions, a second
    // labelset, a single-sample window edge case, and a stale marker that the
    // matrix path drops.
    let mut store = InMemoryMetricStore::new();
    let stale_bits = f64::from_bits(0x7ff0_0000_0000_0002);
    push_float_series(
        &mut store,
        [
            LabeledFloatSeries {
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "queue_depth"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                ]),
                samples: vec![
                    FloatSampleInput {
                        timestamp_ms: 60_000,
                        value: 2.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 120_000,
                        value: 4.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 180_000,
                        value: 4.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 240_000,
                        value: 5.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 300_000,
                        value: 9.0,
                    },
                ],
            },
            LabeledFloatSeries {
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "queue_depth"),
                    crate::test_support::TestLabelPair::new("job", "db"),
                ]),
                samples: vec![
                    FloatSampleInput {
                        timestamp_ms: 120_000,
                        value: 3.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 240_000,
                        value: 7.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 300_000,
                        value: 1.0,
                    },
                ],
            },
            LabeledFloatSeries {
                // A stale marker mid-window: dropped by both paths.
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "queue_depth"),
                    crate::test_support::TestLabelPair::new("job", "stale"),
                ]),
                samples: vec![
                    FloatSampleInput {
                        timestamp_ms: 120_000,
                        value: 5.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 180_000,
                        value: stale_bits,
                    },
                    FloatSampleInput {
                        timestamp_ms: 300_000,
                        value: 6.0,
                    },
                ],
            },
            LabeledFloatSeries {
                // Single in-window sample: rate yields no value, but over_time
                // reductions (avg/min/max/last/...) do.
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "queue_depth"),
                    crate::test_support::TestLabelPair::new("job", "lonely"),
                ]),
                samples: vec![FloatSampleInput {
                    timestamp_ms: 295_000,
                    value: 100.0,
                }],
            },
            // A `g`-grouped family for the SPARSE aggregate-over-over_time case at a
            // TIGHT `[30s]` window closing on t=300000 (window (270k, 300k]):
            //   g="mix": a member WITH an in-window sample (300k) -> has a value,
            //     plus a member whose only sample (120k) is outside the window ->
            //     no value (NULL). The no-value member is excluded, so the group
            //     survives with only the in-window member.
            //   g="allsparse": every member's only sample is outside the window,
            //     so the whole group is no-value and produces NO result row.
            depth_g_series("mix", "0", 300_000, 5.0),
            depth_g_series("mix", "1", 120_000, 9.0),
            depth_g_series("allsparse", "0", 120_000, 1.0),
            depth_g_series("allsparse", "1", 120_000, 2.0),
        ],
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries = [
        OverTimeQueryCase::new("avg_over_time(queue_depth[5m])", 300_000),
        OverTimeQueryCase::new("sum_over_time(queue_depth[5m])", 300_000),
        OverTimeQueryCase::new("count_over_time(queue_depth[5m])", 300_000),
        OverTimeQueryCase::new("min_over_time(queue_depth[5m])", 300_000),
        OverTimeQueryCase::new("max_over_time(queue_depth[5m])", 300_000),
        OverTimeQueryCase::new("stddev_over_time(queue_depth[5m])", 300_000),
        OverTimeQueryCase::new("stdvar_over_time(queue_depth[5m])", 300_000),
        // `last_over_time` preserves the metric name; every other family drops it.
        OverTimeQueryCase::new("last_over_time(queue_depth[5m])", 300_000),
        OverTimeQueryCase::new("present_over_time(queue_depth[5m])", 300_000),
        OverTimeQueryCase::new("quantile_over_time(0.5, queue_depth[5m])", 300_000),
        OverTimeQueryCase::new("quantile_over_time(0.9, queue_depth[5m])", 300_000),
        // @ and offset on the matrix selector exercise the time modifier.
        OverTimeQueryCase::new("avg_over_time(queue_depth[3m] @ 300)", 999_999),
        OverTimeQueryCase::new("sum_over_time(queue_depth[4m] offset 1m)", 360_000),
        // Tighter window that strands the single-sample series for some fns.
        OverTimeQueryCase::new("min_over_time(queue_depth[90s])", 300_000),
        // EXPERIMENTAL over_time members now route through the shared-kernel
        // operator path. `first_over_time` preserves `__name__`; the `ts_of_*`
        // family returns the matching sample's timestamp in seconds.
        OverTimeQueryCase::new("mad_over_time(queue_depth[5m])", 300_000),
        OverTimeQueryCase::new("first_over_time(queue_depth[5m])", 300_000),
        OverTimeQueryCase::new("ts_of_min_over_time(queue_depth[5m])", 300_000),
        OverTimeQueryCase::new("ts_of_max_over_time(queue_depth[5m])", 300_000),
        OverTimeQueryCase::new("ts_of_first_over_time(queue_depth[5m])", 300_000),
        OverTimeQueryCase::new("ts_of_last_over_time(queue_depth[5m])", 300_000),
        // Experimental members composed under an aggregation also route.
        OverTimeQueryCase::new("sum by (job) (mad_over_time(queue_depth[5m]))", 300_000),
        OverTimeQueryCase::new(
            "count by (job) (ts_of_max_over_time(queue_depth[5m]))",
            300_000,
        ),
        // @ / offset on an experimental member.
        OverTimeQueryCase::new("first_over_time(queue_depth[3m] @ 300)", 999_999),
        // Aggregation over over_time: the compositional operator-path case.
        OverTimeQueryCase::new("sum by (job) (avg_over_time(queue_depth[5m]))", 300_000),
        OverTimeQueryCase::new(
            "max without (job) (last_over_time(queue_depth[5m]))",
            300_000,
        ),
        // SPARSE aggregate-over-over_time: a group mixing an in-window member
        // with a no-value (stranded) member excludes the no-value member, and
        // an all-no-value group produces no result row. Every op must agree
        // with the interpreter.
        OverTimeQueryCase::new("sum by (g) (avg_over_time(depth_g[30s]))", 300_000),
        OverTimeQueryCase::new("count by (g) (avg_over_time(depth_g[30s]))", 300_000),
        OverTimeQueryCase::new("min by (g) (max_over_time(depth_g[30s]))", 300_000),
    ];

    for case in queries {
        let parity = planned_and_interpreted(&engine, case.query, case.time_ms).await;
        let via_operators = parity.via_operators;
        let via_interpreter = parity.via_interpreter;

        let normalize = |result: QueryResult| fingerprint_sorted(result, case.query);

        let via_interpreter = normalize(via_interpreter);
        let via_operators = normalize(via_operators);
        // NaN-aware comparison (a genuine NaN reduction equals itself).
        assert2::assert!(instant_samples_match(&via_interpreter, &via_operators));

        // Pin the SPARSE aggregate-over-over_time rule: the no-value (stranded)
        // member is excluded from its group, and the all-no-value group is
        // absent.
        if matches!(
            case.query,
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
            if case.query == "count by (g) (avg_over_time(depth_g[30s]))" {
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

}

mod param_aggregate_planner_path_matches_interpreter {
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

type ParameterizedQueryCase = QueryAt;

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

    assert_call_planner_matches_interpreter(&engine, &queries).await;

    // The experimental `limitk`/`limit_ratio` param aggregations now route
    // through the planner via the shared interpreter kernels (incl.
    // `limit_ratio`'s `InvalidRatioWarning`); their parity is checked in
    // `experimental_param_aggregate_planner_path_matches_interpreter`.
}

}

mod rate_range_planner_path_matches_interpreter {
use super::*;

struct RateRangeCase {
    query: &'static str,
    time_ms: i64,
}

#[tokio::test]
pub(crate) async fn rate_range_planner_path_matches_interpreter() {
    use promql_parser::parser::Expr;

    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // Float-only counters with a reset, a gauge for delta, an offset
    // history, and a single-sample series (no rate value).
    let mut store = InMemoryMetricStore::new();
    push_float_series(
        &mut store,
        [
            LabeledFloatSeries {
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                ]),
                samples: vec![
                    FloatSampleInput {
                        timestamp_ms: 0,
                        value: 0.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 60_000,
                        value: 1.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 120_000,
                        value: 2.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 180_000,
                        value: 3.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 240_000,
                        value: 4.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 300_000,
                        value: 5.0,
                    },
                ],
            },
            LabeledFloatSeries {
                // A counter reset mid-window (5 -> 1) exercises reset correction.
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
                    crate::test_support::TestLabelPair::new("job", "db"),
                ]),
                samples: vec![
                    FloatSampleInput {
                        timestamp_ms: 0,
                        value: 0.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 60_000,
                        value: 3.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 120_000,
                        value: 5.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 180_000,
                        value: 1.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 240_000,
                        value: 4.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 300_000,
                        value: 8.0,
                    },
                ],
            },
            LabeledFloatSeries {
                // A gauge with ups and downs for delta/idelta.
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "temperature"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                ]),
                samples: vec![
                    FloatSampleInput {
                        timestamp_ms: 180_000,
                        value: 10.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 240_000,
                        value: 7.0,
                    },
                    FloatSampleInput {
                        timestamp_ms: 300_000,
                        value: 9.0,
                    },
                ],
            },
            LabeledFloatSeries {
                // Single sample in-window: rate-family yields no value. Both paths
                // must DROP this series identically (NULL-drop on the operator
                // path, no-value omission on the interpreter).
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
                    crate::test_support::TestLabelPair::new("job", "lonely"),
                ]),
                samples: vec![FloatSampleInput {
                    timestamp_ms: 295_000,
                    value: 100.0,
                }],
            },
            LabeledFloatSeries {
                // A gauge whose window holds a GENUINE NaN sample: `delta` computes
                // a value (the window is non-empty with >=2 samples), and the
                // arithmetic yields NaN. That NaN is a real value (non-null), so it
                // must be KEPT and propagated on both paths — not dropped.
                labels: labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "nan_gauge"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                ]),
                samples: vec![
                    FloatSampleInput {
                        timestamp_ms: 240_000,
                        value: f64::NAN,
                    },
                    FloatSampleInput {
                        timestamp_ms: 300_000,
                        value: 5.0,
                    },
                ],
            },
        ],
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries = [
        RateRangeCase {
            query: "rate(http_requests_total[5m])",
            time_ms: 300_000,
        },
        RateRangeCase {
            query: "increase(http_requests_total[5m])",
            time_ms: 300_000,
        },
        RateRangeCase {
            query: "delta(temperature[2m])",
            time_ms: 300_000,
        },
        RateRangeCase {
            query: "irate(http_requests_total[5m])",
            time_ms: 300_000,
        },
        RateRangeCase {
            query: "idelta(http_requests_total[5m])",
            time_ms: 300_000,
        },
        // @ and offset on the matrix selector exercise the time modifier.
        RateRangeCase {
            query: "rate(http_requests_total[3m] @ 300)",
            time_ms: 999_999,
        },
        RateRangeCase {
            query: "increase(http_requests_total[4m] offset 1m)",
            time_ms: 360_000,
        },
        // Tighter window that strands the single-sample series.
        RateRangeCase {
            query: "rate(http_requests_total{job=\"api\"}[90s])",
            time_ms: 300_000,
        },
        // A genuine-NaN delta: the computed value is NaN but non-null, so the
        // series is KEPT (not dropped). Both paths must agree, NaN-aware.
        RateRangeCase {
            query: "delta(nan_gauge[2m])",
            time_ms: 300_000,
        },
    ];

    for query_index in 0..queries.len() {
        let case = &queries[query_index];
        let query = case.query;
        let time_ms = case.time_ms;
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let Expr::Call(_) = &expr else {
            panic!("`{query}` did not parse to a call");
        };
        let call = match_rate_range_call(&expr)
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
            .eval_rate_range_via_planner(
                "t",
                MatrixSelectorAt {
                    selector: call.selector,
                    time_ms,
                },
                call.kind,
            )
            .await
            .unwrap_or_else(|error| panic!("planner `{query}`: {error}"));

        let normalize = |result: QueryResult| fingerprint_sorted(result, query);

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

}

mod simple_aggregate_planner_path_matches_interpreter {
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

}

mod subquery_planner_path_matches_interpreter {
use super::*;

#[derive(bon::Builder)]
struct CounterSeriesFixture {
    label: &'static str,
    factor: f64,
}

#[derive(bon::Builder)]
struct GaugeSeriesFixture {
    label: &'static str,
    group: &'static str,
    base: f64,
}

struct SubqueryCase {
    query: &'static str,
    timestamp_ms: i64,
}

/// Differential parity for subqueries routed through the recursive planner.
///
/// The query is a range call or a `*_over_time` call whose argument is
/// `inner[range:res]`. The planner builds the subquery range vector per
/// aligned sub-step and then applies the shared outer fold. The result must
/// equal the interpreter's `eval_subquery` plus outer fold byte-for-byte, with
/// a NaN-aware comparison.
#[tokio::test]
pub(crate) async fn subquery_planner_path_matches_interpreter() {
    // A float-only store exercising the subquery sub-grid:
    //  - `reqs_total{l}`: two counters (l=a, l=b) for rate/over_time-of-rate.
    //  - `gauge{l}`: a plain gauge in two label groups for the aggregating
    //    inner (`sum by(l)`).
    //  - `sparse{l}`: a series with a single early sample so a tight subquery
    //    window strands it (no-value sub-grid -> dropped series), plus a dense
    //    member so the surviving series is observable.
    let mut store = InMemoryMetricStore::new();
    // Counters: slope = factor over 60s, sampled every 30s out to 20m.
    for fixture in [
        CounterSeriesFixture::builder()
            .label("a")
            .factor(1.0)
            .build(),
        CounterSeriesFixture::builder()
            .label("b")
            .factor(2.0)
            .build(),
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "reqs_total"),
            crate::test_support::TestLabelPair::new("l", fixture.label),
        ]);
        for step in 0..=40_i32 {
            store.push_float(
                "t",
                lbls.clone(),
                i64::from(step) * 30_000,
                f64::from(step) * fixture.factor,
            );
        }
    }
    // Gauges in two groups (`sum by(l)` collapses the `g` member dimension).
    for fixture in [
        GaugeSeriesFixture::builder()
            .label("a")
            .group("0")
            .base(3.0)
            .build(),
        GaugeSeriesFixture::builder()
            .label("a")
            .group("1")
            .base(5.0)
            .build(),
        GaugeSeriesFixture::builder()
            .label("b")
            .group("0")
            .base(7.0)
            .build(),
        GaugeSeriesFixture::builder()
            .label("b")
            .group("1")
            .base(11.0)
            .build(),
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "gauge"),
            crate::test_support::TestLabelPair::new("l", fixture.label),
            crate::test_support::TestLabelPair::new("g", fixture.group),
        ]);
        for step in 0..=40_i32 {
            store.push_float(
                "t",
                lbls.clone(),
                i64::from(step) * 30_000,
                fixture.base + f64::from(step),
            );
        }
    }
    // Sparse: l=dense has a full history; l=stranded has only one early
    // sample, so a tight late subquery window yields it no sub-grid points.
    {
        let dense = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "sparse"),
            crate::test_support::TestLabelPair::new("l", "dense"),
        ]);
        for step in 0..=40_i32 {
            store.push_float(
                "t",
                dense.clone(),
                i64::from(step) * 30_000,
                f64::from(step),
            );
        }
        let stranded = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "sparse"),
            crate::test_support::TestLabelPair::new("l", "stranded"),
        ]);
        store.push_float("t", stranded, 0, 1.0);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Each query must route through the operator path and match the
    // interpreter byte-for-byte. `EngineOpts::default().eval_interval` is
    // 60s, so a subquery written `[range:]` (no resolution) uses a 60s stride
    // on BOTH paths.
    let queries = [
        // Selector inner, explicit resolution.
        SubqueryCase {
            query: "rate(reqs_total[5m:1m])",
            timestamp_ms: 1_200_000_i64,
        },
        // Nested: `*_over_time` over a `rate(...)` subquery — the inner rate is
        // itself planned per sub-step.
        SubqueryCase {
            query: "max_over_time(rate(reqs_total[1m])[10m:2m])",
            timestamp_ms: 1_200_000,
        },
        // Aggregating inner with DEFAULT resolution (`[5m:]` -> 60s stride).
        SubqueryCase {
            query: "sum_over_time((sum by(l)(gauge))[5m:])",
            timestamp_ms: 1_200_000,
        },
        SubqueryCase {
            query: "avg_over_time((sum by(l)(gauge))[5m:])",
            timestamp_ms: 1_200_000,
        },
        // `@` and offset on the subquery shift the evaluated end (and the
        // step-aligned start) identically on both paths.
        SubqueryCase {
            query: "sum_over_time(gauge[5m:1m] @ 600)",
            timestamp_ms: 1_200_000,
        },
        SubqueryCase {
            query: "sum_over_time(gauge[5m:1m] offset 5m)",
            timestamp_ms: 1_200_000,
        },
        // Sparse: the stranded member yields an empty sub-grid window and is
        // dropped from the result; the dense member survives. A tight window
        // at a late time.
        SubqueryCase {
            query: "sum_over_time(sparse[1m:30s])",
            timestamp_ms: 1_200_000,
        },
        SubqueryCase {
            query: "last_over_time(sparse[1m:30s])",
            timestamp_ms: 1_200_000,
        },
        // Binary inner.
        SubqueryCase {
            query: "rate((reqs_total + reqs_total)[5m:1m])",
            timestamp_ms: 1_200_000,
        },
        // Unary-negation inner: `Expr::Unary` now routes through the planner,
        // so the subquery's structural gate accepts it and the inner negation
        // is planned per sub-step.
        SubqueryCase {
            query: "sum_over_time((-gauge)[5m:1m])",
            timestamp_ms: 1_200_000,
        },
        SubqueryCase {
            query: "max_over_time((-gauge)[5m:1m])",
            timestamp_ms: 1_200_000,
        },
    ];

    for case in queries {
        let query = case.query;
        let SortedParity {
            via_operators,
            via_interpreter,
        } = sorted_planned_and_interpreted(&engine, query, case.timestamp_ms).await;
        assert2::assert!(instant_samples_match(&via_interpreter, &via_operators));

        // Pin the sparse-window rule: the stranded member is dropped (no
        // sub-grid points), so only the dense series survives.
        if query == "sum_over_time(sparse[1m:30s])" {
            assert2::assert!(via_operators.len() == 1);
            assert2::assert!(via_operators[0].labels.get("l") == Some("dense"));
        }
    }
}

}
