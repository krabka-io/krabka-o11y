use super::*;

#[derive(bon::Builder)]
struct FloatAggregationSample {
    instance: &'static str,
    value: f64,
}

struct AggregationQueryCase {
    query: &'static str,
    timestamp_ms: i64,
}

/// Differential parity for histogram-bearing aggregations in the planner.
///
/// The recursive planner routes these aggregations through the shared
/// `apply_simple_aggregate` and `apply_*` kernels, which is the `Precomputed`
/// path. Each query MUST claim the operator path and MUST match the interpreter
/// byte-for-byte. The match covers the native-histogram payloads, which the
/// test compares structurally and not by float `==`, and any warning or info
/// annotation.
///
/// The store exercises every native-histogram aggregation rule:
/// - `sum`/`avg` merge compatible histograms, and `avg` scales by `1/count`;
/// - `sum`/`avg` drop a group that mixes a float and a histogram, which is the
///   mixed-sample rule;
/// - `count`/`group` count every sample regardless of type;
/// - `min`/`max`/`stddev`/`stdvar`/`topk`/`bottomk`/`quantile` ignore and drop
///   histogram samples, and reduce only the floats;
/// - `count_values` formats a histogram value as Prometheus bucket notation.
#[tokio::test]
pub(crate) async fn histogram_aggregation_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A native histogram with two positive buckets so the merge / quantile
    // folds produce non-trivial structure.
    fn seed_histogram(count: f64, sum: f64) -> NativeHistogram {
        let mut histogram = native_histogram(count, sum);
        histogram.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 2,
        }];
        histogram.positive_counts = vec![1.0, 3.0];
        histogram
    }

    let mut store = InMemoryMetricStore::new();
    // Group g="hist": TWO compatible native histograms (so sum/avg actually
    // merge), across an `instance` dimension so `without (instance)` collapses
    // them into one group.
    store.push_histogram(
        "t",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "m"),
            crate::test_support::TestLabelPair::new("g", "hist"),
            crate::test_support::TestLabelPair::new("instance", "0"),
        ]),
        300_000,
        seed_histogram(4.0, 6.0),
    );
    store.push_histogram(
        "t",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "m"),
            crate::test_support::TestLabelPair::new("g", "hist"),
            crate::test_support::TestLabelPair::new("instance", "1"),
        ]),
        300_000,
        seed_histogram(8.0, 20.0),
    );
    // Group g="float": TWO float members (so the float aggregations reduce a
    // real group and `count`/`group` see floats).
    for sample in [
        FloatAggregationSample::builder()
            .instance("0")
            .value(2.0)
            .build(),
        FloatAggregationSample::builder()
            .instance("1")
            .value(6.0)
            .build(),
    ] {
        store.push_float(
            "t",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "m"),
                crate::test_support::TestLabelPair::new("g", "float"),
                crate::test_support::TestLabelPair::new("instance", sample.instance),
            ]),
            300_000,
            sample.value,
        );
    }
    // Group g="mixed": ONE float + ONE histogram in the same group. Under
    // `sum`/`avg` this group is dropped (mixed-sample rule); under
    // `count`/`group` it counts 2; under the histogram-ignoring ops only the
    // float survives.
    store.push_float(
        "t",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "m"),
            crate::test_support::TestLabelPair::new("g", "mixed"),
            crate::test_support::TestLabelPair::new("instance", "0"),
        ]),
        300_000,
        10.0,
    );
    store.push_histogram(
        "t",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "m"),
            crate::test_support::TestLabelPair::new("g", "mixed"),
            crate::test_support::TestLabelPair::new("instance", "1"),
        ]),
        300_000,
        seed_histogram(2.0, 3.0),
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries = [
        // sum/avg MERGE histograms per group; the mixed group is dropped.
        AggregationQueryCase {
            query: "sum by (g) (m)",
            timestamp_ms: 300_000,
        },
        AggregationQueryCase {
            query: "avg by (g) (m)",
            timestamp_ms: 300_000,
        },
        AggregationQueryCase {
            query: "sum without (instance) (m)",
            timestamp_ms: 300_000,
        },
        AggregationQueryCase {
            query: "avg without (instance) (m)",
            timestamp_ms: 300_000,
        },
        // Global sum over everything: the lone global group mixes floats and
        // histograms, so it is dropped entirely (empty result).
        AggregationQueryCase {
            query: "sum(m)",
            timestamp_ms: 300_000,
        },
        // count/group count every sample regardless of type.
        AggregationQueryCase {
            query: "count by (g) (m)",
            timestamp_ms: 300_000,
        },
        AggregationQueryCase {
            query: "group by (g) (m)",
            timestamp_ms: 300_000,
        },
        AggregationQueryCase {
            query: "count without (instance) (m)",
            timestamp_ms: 300_000,
        },
        AggregationQueryCase {
            query: "count(m)",
            timestamp_ms: 300_000,
        },
        // min/max/stddev/stdvar IGNORE histograms (reduce only floats); the
        // all-histogram g="hist" group produces no row, g="float" reduces its
        // two floats, g="mixed" reduces just its one float.
        AggregationQueryCase {
            query: "min by (g) (m)",
            timestamp_ms: 300_000,
        },
        AggregationQueryCase {
            query: "max by (g) (m)",
            timestamp_ms: 300_000,
        },
        AggregationQueryCase {
            query: "stddev by (g) (m)",
            timestamp_ms: 300_000,
        },
        AggregationQueryCase {
            query: "stdvar by (g) (m)",
            timestamp_ms: 300_000,
        },
        // topk/bottomk/quantile also IGNORE histograms.
        AggregationQueryCase {
            query: "topk by (g) (1, m)",
            timestamp_ms: 300_000,
        },
        AggregationQueryCase {
            query: "bottomk by (g) (1, m)",
            timestamp_ms: 300_000,
        },
        AggregationQueryCase {
            query: "quantile by (g) (0.5, m)",
            timestamp_ms: 300_000,
        },
        // count_values formats histogram values as Prometheus bucket notation.
        AggregationQueryCase {
            query: "count_values by (g) (\"v\", m)",
            timestamp_ms: 300_000,
        },
    ];

    for case in queries {
        assert_annotated_call_planner_matches_interpreter(&engine, case.query, case.timestamp_ms)
            .await;
    }

    // Pin the absolute histogram-aware rules (not just operator==interpreter).
    let sample_by_group =
        |samples: &[crate::InstantSample], g: &str| -> Option<crate::InstantSample> {
            samples
                .iter()
                .find(|sample| sample.labels.get("g") == Some(g))
                .cloned()
        };

    // `sum by (g) (m)`: g="hist" is the MERGED histogram (count 4+8=12,
    // sum 6+20=26), g="float" sums its two floats (2+6=8), g="mixed" is
    // DROPPED (float+histogram).
    let sum_expr =
        parse_promql_with_duration_context("sum by (g) (m)", DurationExprContext::instant(300_000))
            .expect("parse sum");
    let plan = engine
        .plan_instant_expr("t", &sum_expr, 300_000)
        .await
        .expect("plan sum")
        .expect("sum routes through planner");
    let QueryResult::InstantVector(sum_samples) = engine
        .assemble_planned_instant(plan, 300_000)
        .await
        .expect("assemble sum")
    else {
        panic!("expected vector for sum");
    };
    assert2::assert!(sample_by_group(&sum_samples, "mixed").is_none());
    let hist_row = sample_by_group(&sum_samples, "hist").expect("sum: g=hist row");
    let SampleValue::Histogram(merged) = hist_row.value else {
        panic!("sum: g=hist must be a merged histogram, got: {hist_row:?}");
    };
    assert2::assert!(approx_eq(merged.count, 12.0) && approx_eq(merged.sum, 26.0));
    let float_row = sample_by_group(&sum_samples, "float").expect("sum: g=float row");
    assert2::assert!(approx_eq(float_value(&float_row.value), 8.0));

    // `min by (g) (m)`: g="hist" (all histograms) yields NO row; g="float"
    // reduces to 2; g="mixed" reduces to just its one float (10).
    let min_expr =
        parse_promql_with_duration_context("min by (g) (m)", DurationExprContext::instant(300_000))
            .expect("parse min");
    let plan = engine
        .plan_instant_expr("t", &min_expr, 300_000)
        .await
        .expect("plan min")
        .expect("min routes through planner");
    let QueryResult::InstantVector(min_samples) = engine
        .assemble_planned_instant(plan, 300_000)
        .await
        .expect("assemble min")
    else {
        panic!("expected vector for min");
    };
    check!(
        sample_by_group(&min_samples, "hist").is_none(),
        "min: all-histogram group must be absent (histograms ignored), got: {min_samples:?}"
    );
    check!(
        approx_eq(
            float_value(
                &sample_by_group(&min_samples, "float")
                    .expect("min g=float")
                    .value
            ),
            2.0
        ),
        "min: g=float must be 2"
    );
    check!(
        approx_eq(
            float_value(
                &sample_by_group(&min_samples, "mixed")
                    .expect("min g=mixed")
                    .value
            ),
            10.0
        ),
        "min: g=mixed must reduce just its float (10)"
    );
}
