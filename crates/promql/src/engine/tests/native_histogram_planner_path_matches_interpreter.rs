use super::*;

#[derive(bon::Builder)]
struct ClassicBucketSample {
    upper_bound: &'static str,
    value: f64,
}

/// Differential parity for the native-histogram constructs in the planner.
///
/// The recursive planner routes a bare native-histogram selector, native
/// `histogram_quantile`, and every native accessor: `histogram_count`/`sum`/
/// `avg`/`stddev`/`stdvar`/`fraction`. Each query MUST claim the operator
/// `Precomputed` path and MUST match the interpreter byte-for-byte. The test
/// compares the histogram payloads by value, not by float `==`.
#[tokio::test]
pub(crate) async fn native_histogram_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // Build a non-trivial native histogram (schema 0, two positive buckets
    // [1,2] and [2,4] carrying counts 1 and 3) with a real count/sum so the
    // quantile, fraction, and stddev/stdvar folds all produce finite values.
    fn seed_histogram(count: f64, sum: f64) -> NativeHistogram {
        let mut histogram = native_histogram(count, sum);
        histogram.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 2,
        }];
        histogram.positive_counts = vec![1.0, 3.0];
        histogram
    }

    // Two native-histogram groups (job=api / job=db) so multi-series output and
    // the `__name__` drop are both observable, plus a classic `cls_bucket{le}`
    // float histogram to exercise the classic+native co-routing inside the
    // shared `histogram_quantile` / `histogram_fraction` folds.
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "t",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "nh"),
            crate::test_support::TestLabelPair::new("job", "api"),
        ]),
        300_000,
        seed_histogram(4.0, 6.5),
    );
    store.push_histogram(
        "t",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "nh"),
            crate::test_support::TestLabelPair::new("job", "db"),
        ]),
        300_000,
        seed_histogram(8.0, 20.0),
    );
    for bucket in [
        ClassicBucketSample::builder()
            .upper_bound("1")
            .value(1.0)
            .build(),
        ClassicBucketSample::builder()
            .upper_bound("2")
            .value(3.0)
            .build(),
        ClassicBucketSample::builder()
            .upper_bound("+Inf")
            .value(4.0)
            .build(),
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "cls_bucket"),
            crate::test_support::TestLabelPair::new("le", bucket.upper_bound),
        ]);
        store.push_float("t", lbls, 300_000, bucket.value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries = [
        // The bare native-histogram selector itself (carries the histogram
        // payload + full labelset, including `__name__`).
        QueryAt {
            query: "nh",
            time_ms: 300_000,
        },
        // Native histogram_quantile at two phis.
        QueryAt {
            query: "histogram_quantile(0.5, nh)",
            time_ms: 300_000,
        },
        QueryAt {
            query: "histogram_quantile(0.9, nh)",
            time_ms: 300_000,
        },
        // Every native accessor.
        QueryAt {
            query: "histogram_count(nh)",
            time_ms: 300_000,
        },
        QueryAt {
            query: "histogram_sum(nh)",
            time_ms: 300_000,
        },
        QueryAt {
            query: "histogram_avg(nh)",
            time_ms: 300_000,
        },
        QueryAt {
            query: "histogram_stddev(nh)",
            time_ms: 300_000,
        },
        QueryAt {
            query: "histogram_stdvar(nh)",
            time_ms: 300_000,
        },
        // histogram_fraction carries two scalar bounds.
        QueryAt {
            query: "histogram_fraction(1, 2, nh)",
            time_ms: 300_000,
        },
        QueryAt {
            query: "histogram_fraction(-Inf, +Inf, nh)",
            time_ms: 300_000,
        },
        // The shared folds also work over the classic float buckets.
        QueryAt {
            query: "histogram_quantile(0.5, cls_bucket)",
            time_ms: 300_000,
        },
        QueryAt {
            query: "histogram_fraction(1, 2, cls_bucket)",
            time_ms: 300_000,
        },
    ];

    assert_call_planner_matches_interpreter(&engine, &queries).await;

    // The bare-selector case must surface the native histogram payload (proving
    // the histogram-aware selection actually carried it, not a dropped/empty
    // vector).
    let bare = parse_promql_with_duration_context("nh", DurationExprContext::instant(300_000))
        .expect("parse nh");
    let plan = engine
        .plan_instant_expr("t", &bare, 300_000)
        .await
        .expect("plan nh")
        .expect("nh routes through planner");
    let QueryResult::InstantVector(samples) = engine
        .assemble_planned_instant(plan, 300_000)
        .await
        .expect("assemble nh")
    else {
        panic!("expected vector for nh");
    };
    assert2::assert!(samples.len() == 2);
    assert2::assert!(
        samples
            .iter()
            .all(|sample| matches!(sample.value, SampleValue::Histogram(_)))
    );

    // `histogram_quantiles` (experimental) now routes through the shared
    // `apply_histogram_quantiles` fold and must match the interpreter for both
    // native-histogram and classic bucket inputs, across multiple phis.
    #[cfg(feature = "experimental-functions")]
    for query in [
        "histogram_quantiles(nh, \"q\", 0.5, 0.9)",
        "histogram_quantiles(cls_bucket, \"q\", 0.5)",
    ] {
        let parity = planned_and_interpreted(&engine, query, 300_000).await;
        let via_operators = parity.via_operators;
        let via_interpreter = parity.via_interpreter;
        let normalize = |result: QueryResult| fingerprint_sorted(result, query);
        assert2::assert!(instant_samples_match(
            &normalize(via_interpreter),
            &normalize(via_operators)
        ));
    }
}
