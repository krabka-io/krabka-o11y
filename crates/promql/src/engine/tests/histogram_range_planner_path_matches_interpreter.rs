use super::*;

#[tokio::test]
pub(crate) async fn histogram_range_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A native-histogram counter sample with two positive buckets, so the
    // rate/increase/delta extrapolation produces non-trivial per-bucket
    // structure and the over_time merge folds real buckets.
    fn counter_histogram(count: f64, sum: f64, b0: f64, b1: f64) -> NativeHistogram {
        let mut histogram = native_histogram(count, sum);
        histogram.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 2,
        }];
        histogram.positive_counts = vec![b0, b1];
        histogram
    }

    // A single histogram-counter series sampled monotonically over a 10m
    // window, then a COUNTER RESET (all components drop) so the planner must
    // exercise the shared counter-reset + extrapolation rules. Timestamps are
    // 1m apart so the window `(eval-10m, eval]` captures the full series.
    let mut store = InMemoryMetricStore::new();
    let series = labels(&[("__name__", "h"), ("job", "api")]);
    for (ts, count, sum, b0, b1) in [
        (60_000_i64, 4.0, 6.0, 1.0, 3.0),
        (120_000, 6.0, 10.0, 2.0, 4.0),
        (180_000, 9.0, 15.0, 3.0, 6.0),
        // COUNTER RESET: every component decreases below the prior sample.
        (240_000, 2.0, 3.0, 1.0, 1.0),
        (300_000, 5.0, 8.0, 2.0, 3.0),
    ] {
        store.push_histogram(
            "t",
            series.clone(),
            ts,
            counter_histogram(count, sum, b0, b1),
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let time_ms = 300_000_i64;

    // Each query is a histogram-bearing rate-family / `_over_time` call (or a
    // composition over one). It must route through the recursive planner (the
    // `Precomputed` path), produce a result byte-for-byte identical to the
    // interpreter (histogram payloads compared structurally, floats
    // bit-exactly), and emit identical annotations.
    let queries = [
        // rate-family over histogram counters (counter-reset + extrapolation).
        "rate(h[10m])",
        "increase(h[10m])",
        "delta(h[10m])",
        "irate(h[10m])",
        "idelta(h[10m])",
        // `_over_time` members that MERGE histograms.
        "sum_over_time(h[10m])",
        "avg_over_time(h[10m])",
        // `_over_time` members that are histogram-SAFE (count the samples /
        // pick the latest, regardless of type).
        "count_over_time(h[10m])",
        "last_over_time(h[10m])",
        "present_over_time(h[10m])",
        // `_over_time` members that IGNORE histograms: an all-histogram window
        // yields no float sample, so the series is dropped (empty result).
        "min_over_time(h[10m])",
        "max_over_time(h[10m])",
        "stddev_over_time(h[10m])",
        "stdvar_over_time(h[10m])",
        "quantile_over_time(0.5, h[10m])",
        // Nested: `histogram_quantile` over `rate(h[range])` composes through
        // the operator path (rate produces a histogram, the quantile folds it).
        "histogram_quantile(0.5, rate(h[10m]))",
        // Aggregation over a histogram rate composes through operators.
        "sum(rate(h[10m]))",
        "sum by (job) (increase(h[10m]))",
    ];

    for query in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        let (via_operators, operator_annotations) = super::super::ANNOTATIONS
            .scope(std::cell::RefCell::new(crate::Annotations::new()), async {
                let plan = engine
                    .plan_instant_expr("t", &expr, time_ms)
                    .await
                    .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                    .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
                let result = engine
                    .assemble_planned_instant(plan, time_ms)
                    .await
                    .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));
                let annotations = super::super::ANNOTATIONS.with(|sink| sink.borrow().clone());
                (result, annotations)
            })
            .await;

        let (via_interpreter, interpreter_annotations) = super::super::ANNOTATIONS
            .scope(std::cell::RefCell::new(crate::Annotations::new()), async {
                let result = engine
                    .eval_instant_expr("t", &expr, time_ms)
                    .await
                    .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
                let annotations = super::super::ANNOTATIONS.with(|sink| sink.borrow().clone());
                (result, annotations)
            })
            .await;

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
        assert2::assert!(operator_annotations == interpreter_annotations);
    }

    // Pin the absolute rules the parity above relies on (not just
    // operator==interpreter).

    // `rate(h[10m])` yields ONE histogram sample (name dropped), built by the
    // shared counter-reset + extrapolation rules.
    let rate_expr =
        parse_promql_with_duration_context("rate(h[10m])", DurationExprContext::instant(time_ms))
            .expect("parse rate");
    let plan = engine
        .plan_instant_expr("t", &rate_expr, time_ms)
        .await
        .expect("plan rate")
        .expect("rate routes through planner");
    let QueryResult::InstantVector(rate_samples) = engine
        .assemble_planned_instant(plan, time_ms)
        .await
        .expect("assemble rate")
    else {
        panic!("expected vector for rate");
    };
    assert2::assert!(rate_samples.len() == 1);
    assert2::assert!(rate_samples[0].labels.get("__name__") == None);
    assert2::assert!(matches!(rate_samples[0].value, SampleValue::Histogram(_)));

    // `min_over_time(h[10m])` over an all-histogram window yields NO row
    // (histograms ignored).
    let min_expr = parse_promql_with_duration_context(
        "min_over_time(h[10m])",
        DurationExprContext::instant(time_ms),
    )
    .expect("parse min_over_time");
    let plan = engine
        .plan_instant_expr("t", &min_expr, time_ms)
        .await
        .expect("plan min_over_time")
        .expect("min_over_time routes through planner");
    let QueryResult::InstantVector(min_samples) = engine
        .assemble_planned_instant(plan, time_ms)
        .await
        .expect("assemble min_over_time")
    else {
        panic!("expected vector for min_over_time");
    };
    assert2::assert!(min_samples.is_empty());
}
