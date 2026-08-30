use super::*;

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
    for (idx, value) in bz_values.iter().enumerate() {
        let lbls = labels(&[
            ("__name__", "bz_total"),
            ("g", "all"),
            ("instance", &idx.to_string()),
        ]);
        store.push_float("t", lbls, 120_000, *value);
    }

    // Counters for the rate-then-sum/avg case, mirroring the audit's 2m-window
    // rates: a multi-series group whose per-series rates sum with
    // float-rounding sensitivity.
    for (instance, factor) in [
        ("0", 1.0),
        ("1", 1e8),
        ("2", 1e-8),
        ("3", 7.0),
        ("4", 1234.567),
        ("5", -3.5),
    ] {
        let lbls = labels(&[
            ("__name__", "bz_reqs_total"),
            ("g", "all"),
            ("instance", instance),
        ]);
        for step in 0..=2_i32 {
            store.push_float(
                "t",
                lbls.clone(),
                i64::from(step) * 60_000,
                f64::from(step) * factor,
            );
        }
    }

    // A global-fold case mixing a sign-flipped NaN (from `+Inf + -Inf`) with
    // genuine NaNs. `+Inf` and `-Inf` in the same group sum to a NaN whose
    // sign bit is SET (`0xfff8…`) on most platforms, distinct from a genuine
    // payload NaN (`0x7ff8…`). The fold order determines which NaN's bits
    // survive, so the operator path must agree with the interpreter bit-for-
    // bit on the sign bit.
    for (instance, value) in [
        ("a", f64::INFINITY),
        ("b", f64::NEG_INFINITY),
        ("c", f64::NAN),
        ("d", 5.0),
    ] {
        let lbls = labels(&[("__name__", "naninf"), ("instance", instance)]);
        store.push_float("t", lbls, 120_000, value);
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // (query, time_ms). Each is a collapsed/global or `by (g)` sum/avg over a
    // multi-series group, plus the rate-wrapped and naninf cases.
    let cases = [
        ("sum(bz_total)", 120_000_i64),
        ("avg(bz_total)", 120_000),
        ("sum by (g) (bz_total)", 120_000),
        ("avg by (g) (bz_total)", 120_000),
        ("sum(rate(bz_reqs_total[2m]))", 120_000),
        ("avg(rate(bz_reqs_total[2m]))", 120_000),
        ("sum by (g) (rate(bz_reqs_total[2m]))", 120_000),
        ("avg by (g) (rate(bz_reqs_total[2m]))", 120_000),
        ("sum(naninf)", 120_000),
        ("avg(naninf)", 120_000),
    ];

    for (query, time_ms) in cases {
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
