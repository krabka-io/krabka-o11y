use super::*;

struct InstanceFactorCase {
    instance: &'static str,
    factor: f64,
}

struct InstanceValueCase {
    instance: &'static str,
    value: f64,
}

struct AggregationQueryCase {
    query: &'static str,
    time_ms: i64,
}

/// Divergences A + B: a collapsed `sum` or `avg` must be deterministic and exact.
///
/// The aggregation runs over a multi-series group. The result must be
/// deterministic run-to-run, bit-exact through `to_bits`. The result must also
/// be bit-for-bit identical to the interpreter oracle. This covers the
/// NaN-sign-bit case, where the sign-flipped NaN `0xfff8…` of a `{+Inf,-Inf}`
/// group folds together with the genuine NaNs `0x7ff8…`. A non-deterministic
/// `DataFusion` hash-aggregate fold flickers by 1 ULP or flips the NaN sign
/// bit. The shared `apply_simple_aggregate` kernel must not do either.
#[tokio::test]
pub(crate) async fn sum_avg_collapsed_is_deterministic_and_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();

    // A multi-series group whose float values sum with float-rounding
    // sensitivity: many members of widely different magnitudes, so the
    // accumulation order changes the low bits of the running sum. The
    // operator fold must pick a single canonical order so the result never
    // flickers and equals the interpreter's stable fold.
    let bz_values = [
        1.0,
        1e16,
        -1e16,
        3.0,
        1e-16,
        7.0,
        -2.5,
        1e8,
        -1e8,
        0.1,
        0.2,
        0.3,
        123_456.789,
        -987_654.321,
        2.617_281_828,
        3.041_592_653,
        -1.314_213_562,
        1e10,
        -1e10,
        42.0,
    ];
    for index in 0..bz_values.len() {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "bz_total"),
            crate::test_support::TestLabelPair::new("g", "all"),
            crate::test_support::TestLabelPair::new("instance", &index.to_string()),
        ]);
        store.push_float("t", lbls, 120_000, bz_values[index]);
    }

    // Counters for the rate-then-sum/avg case, mirroring the audit's 2m-window
    // rates: a multi-series group whose per-series rates sum with
    // float-rounding sensitivity.
    for case in [
        InstanceFactorCase {
            instance: "0",
            factor: 1.0,
        },
        InstanceFactorCase {
            instance: "1",
            factor: 1e8,
        },
        InstanceFactorCase {
            instance: "2",
            factor: 1e-8,
        },
        InstanceFactorCase {
            instance: "3",
            factor: 7.0,
        },
        InstanceFactorCase {
            instance: "4",
            factor: 1234.567,
        },
        InstanceFactorCase {
            instance: "5",
            factor: -3.5,
        },
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "bz_reqs_total"),
            crate::test_support::TestLabelPair::new("g", "all"),
            crate::test_support::TestLabelPair::new("instance", case.instance),
        ]);
        for step in 0..=2_i32 {
            store.push_float(
                "t",
                lbls.clone(),
                i64::from(step) * 60_000,
                f64::from(step) * case.factor,
            );
        }
    }

    // A global-fold case mixing a sign-flipped NaN (from `+Inf + -Inf`) with
    // genuine NaNs. `+Inf` and `-Inf` in the same group sum to a NaN whose
    // sign bit is SET (`0xfff8…`) on most platforms, distinct from a genuine
    // payload NaN (`0x7ff8…`). The fold order determines which NaN's bits
    // survive, so the operator path must agree with the interpreter bit-for-
    // bit on the sign bit.
    for case in [
        InstanceValueCase {
            instance: "a",
            value: f64::INFINITY,
        },
        InstanceValueCase {
            instance: "b",
            value: f64::NEG_INFINITY,
        },
        InstanceValueCase {
            instance: "c",
            value: f64::NAN,
        },
        InstanceValueCase {
            instance: "d",
            value: 5.0,
        },
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "naninf"),
            crate::test_support::TestLabelPair::new("instance", case.instance),
        ]);
        store.push_float("t", lbls, 120_000, case.value);
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Each query is a collapsed/global or `by (g)` sum/avg over a multi-series
    // group, plus the rate-wrapped and naninf cases.
    let cases = [
        AggregationQueryCase {
            query: "sum(bz_total)",
            time_ms: 120_000,
        },
        AggregationQueryCase {
            query: "avg(bz_total)",
            time_ms: 120_000,
        },
        AggregationQueryCase {
            query: "sum by (g) (bz_total)",
            time_ms: 120_000,
        },
        AggregationQueryCase {
            query: "avg by (g) (bz_total)",
            time_ms: 120_000,
        },
        AggregationQueryCase {
            query: "sum(rate(bz_reqs_total[2m]))",
            time_ms: 120_000,
        },
        AggregationQueryCase {
            query: "avg(rate(bz_reqs_total[2m]))",
            time_ms: 120_000,
        },
        AggregationQueryCase {
            query: "sum by (g) (rate(bz_reqs_total[2m]))",
            time_ms: 120_000,
        },
        AggregationQueryCase {
            query: "avg by (g) (rate(bz_reqs_total[2m]))",
            time_ms: 120_000,
        },
        AggregationQueryCase {
            query: "sum(naninf)",
            time_ms: 120_000,
        },
        AggregationQueryCase {
            query: "avg(naninf)",
            time_ms: 120_000,
        },
    ];

    for case in cases {
        let query = case.query;
        let time_ms = case.time_ms;
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // The interpreter oracle: the reference result.
        let interpreter = engine
            .eval_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
        let QueryResult::InstantVector(mut interpreter) = interpreter else {
            panic!("expected vector for interpreter `{query}`");
        };
        interpreter.sort_by_key(|sample| sample.labels.fingerprint());

        // Run the operator path MANY times: every run must be bit-identical
        // (deterministic) and equal the interpreter bit-for-bit.
        let mut first_bits: Option<Vec<u64>> = None;
        for run in 0..50 {
            let plan = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}` (run {run}): {error}"))
                .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
            let operator = engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap_or_else(|error| panic!("operator `{query}` (run {run}): {error}"));
            let QueryResult::InstantVector(mut operator) = operator else {
                panic!("expected vector for operator `{query}`");
            };
            operator.sort_by_key(|sample| sample.labels.fingerprint());

            // Bit-for-bit parity with the interpreter (NaN sign included).
            assert2::assert!(instant_samples_match(&interpreter, &operator));

            // Determinism: capture the exact float bits and require every run
            // to reproduce them.
            let bits: Vec<u64> = operator
                .iter()
                .map(|sample| float_value(&sample.value).to_bits())
                .collect();
            match &first_bits {
                None => first_bits = Some(bits),
                Some(expected) => assert2::assert!(&bits == expected),
            }
        }
    }
}
