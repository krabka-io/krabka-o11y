use super::*;

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
        labels(&[("__name__", "nh"), ("job", "api")]),
        300_000,
        seed_histogram(4.0, 6.5),
    );
    store.push_histogram(
        "t",
        labels(&[("__name__", "nh"), ("job", "db")]),
        300_000,
        seed_histogram(8.0, 20.0),
    );
    for (le, value) in [("1", 1.0), ("2", 3.0), ("+Inf", 4.0)] {
        let lbls = labels(&[("__name__", "cls_bucket"), ("le", le)]);
        store.push_float("t", lbls, 300_000, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries = [
        // The bare native-histogram selector itself (carries the histogram
        // payload + full labelset, including `__name__`).
        ("nh", 300_000_i64),
        // Native histogram_quantile at two phis.
        ("histogram_quantile(0.5, nh)", 300_000),
        ("histogram_quantile(0.9, nh)", 300_000),
        // Every native accessor.
        ("histogram_count(nh)", 300_000),
        ("histogram_sum(nh)", 300_000),
        ("histogram_avg(nh)", 300_000),
        ("histogram_stddev(nh)", 300_000),
        ("histogram_stdvar(nh)", 300_000),
        // histogram_fraction carries two scalar bounds.
        ("histogram_fraction(1, 2, nh)", 300_000),
        ("histogram_fraction(-Inf, +Inf, nh)", 300_000),
        // The shared folds also work over the classic float buckets.
        ("histogram_quantile(0.5, cls_bucket)", 300_000),
        ("histogram_fraction(1, 2, cls_bucket)", 300_000),
    ];

    for (query, time_ms) in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // The recursive planner must claim this query (the `Precomputed` path).
        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(plan, time_ms)
            .await
            .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

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
        // `instant_samples_match` compares histogram payloads structurally
        // (via `SampleValue` `PartialEq`) and floats bit-exactly.
        assert2::assert!(instant_samples_match(&via_interpreter, &via_operators));
    }

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
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(300_000))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let plan = engine
            .plan_instant_expr("t", &expr, 300_000)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(plan, 300_000)
            .await
            .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));
        let via_interpreter = engine
            .eval_instant_expr("t", &expr, 300_000)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };
        assert2::assert!(instant_samples_match(
            &normalize(via_interpreter),
            &normalize(via_operators)
        ));
    }
}
