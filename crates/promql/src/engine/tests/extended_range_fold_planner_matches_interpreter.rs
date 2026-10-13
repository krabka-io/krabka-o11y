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
