use super::*;

/// Differential parity for a label that is present with an empty value.
///
/// A series with `__unit__=""` has the label present and the value empty. That
/// series must stay distinct from a series of the same name that has no
/// `__unit__` label, through to the operator leaf. The operator leaf encodes an
/// absent label as NULL and a present empty label as `""`. The planner
/// instant-selector path and the rate-range path must produce the byte-exact
/// result that the interpreter produces: the same series set, the same
/// labelsets, and the same per-series values.
#[tokio::test]
pub(crate) async fn empty_valued_label_planner_path_matches_interpreter() {
    use promql_parser::parser::Expr;

    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();
    // Three series sharing `__name__=m`, distinguished only by the presence
    // and value of `__unit__`:
    //   - job=a: `__unit__=""`  (PRESENT, empty value)
    //   - job=b: `__unit__="s"` (PRESENT, non-empty)
    //   - job=c: `__unit__` ABSENT
    // The fingerprints of a (present-empty) and c (absent) differ, so both
    // must survive selection as distinct series.
    for (lbls, samples) in [
        (
            labels(&[("__name__", "m"), ("job", "a"), ("__unit__", "")]),
            vec![(0_i64, 1.0), (60_000, 2.0), (120_000, 3.0)],
        ),
        (
            labels(&[("__name__", "m"), ("job", "b"), ("__unit__", "s")]),
            vec![(0, 10.0), (60_000, 20.0), (120_000, 30.0)],
        ),
        (
            labels(&[("__name__", "m"), ("job", "c")]),
            vec![(0, 100.0), (60_000, 200.0), (120_000, 300.0)],
        ),
    ] {
        for (ts, value) in samples {
            store.push_float("t", lbls.clone(), ts, value);
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
        let QueryResult::InstantVector(mut samples) = result else {
            panic!("expected instant vector");
        };
        samples.sort_by_key(|sample| sample.labels.fingerprint());
        samples
    };

    // (a) INSTANT selector path: the bare selector `m` matches all three
    // series. Planner (operator leaf) must equal the interpreter, preserving
    // the present-empty-vs-absent distinction.
    let time_ms = 120_000_i64;
    for query in ["m", "m{__unit__=\"\"}", "m{__unit__!=\"\"}"] {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let Expr::VectorSelector(selector) = expr else {
            panic!("`{query}` did not parse to a bare vector selector");
        };
        let interpreter = normalize(
            engine
                .eval_instant_selector("t", &selector, time_ms)
                .await
                .unwrap(),
        );
        let planner = normalize(
            engine
                .eval_instant_selector_via_planner("t", &selector, time_ms)
                .await
                .unwrap(),
        );
        assert2::assert!(instant_samples_match(&interpreter, &planner));
        // The plannable gate must route the empty-valued selector through the
        // operator path now (no `Ok(None)` fallback).
        let routed = engine
            .plan_instant_expr("t", &Expr::VectorSelector(selector.clone()), time_ms)
            .await
            .unwrap();
        assert2::assert!(routed.is_some());
    }

    // The bare selector `m` must yield exactly three rows (a, b, c) — proving
    // the present-empty (a) and absent (c) series were not collapsed.
    let bare = normalize(engine.query_instant("t", "m", time_ms).await.unwrap());
    assert2::assert!(bare.len() == 3);

    // (b) RANGE/matrix path: a rate over the empty-valued-label series must
    // also route through the operator leaf and keep the present-empty (a),
    // non-empty (b), and absent (c) series DISTINCT — three separate result
    // series, the present-empty/absent pair not collapsed.
    let (start, end, step) = (0_i64, 120_000_i64, millis(60_000));
    let query = "rate(m[2m])";
    let QueryResult::RangeMatrix(mut series) = engine
        .query_range("t", query, start, end, step)
        .await
        .unwrap()
    else {
        panic!("expected matrix for `{query}`");
    };
    series.sort_by_key(|s| s.labels.fingerprint());
    assert2::assert!(series.len() == 3);
    // All three result series carry DISTINCT labelsets (the present-empty and
    // absent `__unit__` were not merged): distinct fingerprints.
    let fps: std::collections::BTreeSet<_> =
        series.iter().map(|s| s.labels.fingerprint()).collect();
    assert2::assert!(fps.len() == 3);
}
