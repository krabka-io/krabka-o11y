use super::*;

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
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();
    // A monotonic-ish counter with a reset, sampled every 30s through t=300000.
    for (job, samples) in [
        (
            "a",
            vec![
                (0_i64, 0.0_f64),
                (30_000, 5.0),
                (60_000, 10.0),
                (90_000, 4.0), // reset
                (120_000, 9.0),
                (150_000, 15.0),
                (180_000, 21.0),
                (210_000, 25.0),
                (240_000, 30.0),
                (270_000, 33.0),
                (300_000, 40.0),
            ],
        ),
        (
            "b",
            vec![
                (0, 100.0),
                (60_000, 90.0),
                (120_000, 80.0),
                (180_000, 70.0),
                (240_000, 60.0),
                (300_000, 50.0),
            ],
        ),
    ] {
        for (ts, value) in samples {
            store.push_float("t", labels(&[("__name__", "ctr"), ("job", job)]), ts, value);
        }
    }
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
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // The planner must claim this query (Some, never None).
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
        let via_operators = normalize(via_operators);
        let via_interpreter = normalize(via_interpreter);
        assert2::assert!(instant_samples_match(&via_operators, &via_interpreter));
    }
}
