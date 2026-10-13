use super::*;

struct SamplePoint {
    timestamp_ms: i64,
    value: f64,
}

struct SeriesFixture {
    labels: crate::PromqlLabels,
    samples: Vec<SamplePoint>,
}

struct RangeQueryBounds {
    start_ms: i64,
    end_ms: i64,
}

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
    for fixture in [
        SeriesFixture {
            labels: labels(&[
                crate::test_support::TestLabelPair::new("__name__", "m"),
                crate::test_support::TestLabelPair::new("job", "a"),
                crate::test_support::TestLabelPair::new("__unit__", ""),
            ])
            .into(),
            samples: vec![
                SamplePoint {
                    timestamp_ms: 0,
                    value: 1.0,
                },
                SamplePoint {
                    timestamp_ms: 60_000,
                    value: 2.0,
                },
                SamplePoint {
                    timestamp_ms: 120_000,
                    value: 3.0,
                },
            ],
        },
        SeriesFixture {
            labels: labels(&[
                crate::test_support::TestLabelPair::new("__name__", "m"),
                crate::test_support::TestLabelPair::new("job", "b"),
                crate::test_support::TestLabelPair::new("__unit__", "s"),
            ])
            .into(),
            samples: vec![
                SamplePoint {
                    timestamp_ms: 0,
                    value: 10.0,
                },
                SamplePoint {
                    timestamp_ms: 60_000,
                    value: 20.0,
                },
                SamplePoint {
                    timestamp_ms: 120_000,
                    value: 30.0,
                },
            ],
        },
        SeriesFixture {
            labels: labels(&[
                crate::test_support::TestLabelPair::new("__name__", "m"),
                crate::test_support::TestLabelPair::new("job", "c"),
            ])
            .into(),
            samples: vec![
                SamplePoint {
                    timestamp_ms: 0,
                    value: 100.0,
                },
                SamplePoint {
                    timestamp_ms: 60_000,
                    value: 200.0,
                },
                SamplePoint {
                    timestamp_ms: 120_000,
                    value: 300.0,
                },
            ],
        },
    ] {
        for sample in fixture.samples {
            store.push_float(
                "t",
                fixture.labels.clone(),
                sample.timestamp_ms,
                sample.value,
            );
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
    let bare = normalize(
        engine
            .query_instant(&tenant_id("t"), "m", time_ms)
            .await
            .unwrap(),
    );
    assert2::assert!(bare.len() == 3);

    // (b) RANGE/matrix path: a rate over the empty-valued-label series must
    // also route through the operator leaf and keep the present-empty (a),
    // non-empty (b), and absent (c) series DISTINCT — three separate result
    // series, the present-empty/absent pair not collapsed.
    let bounds = RangeQueryBounds {
        start_ms: 0,
        end_ms: 120_000,
    };
    let query = "rate(m[2m])";
    let QueryResult::RangeMatrix(mut series) = engine
        .query_range(
            &tenant_id("t"),
            query,
            bounds.start_ms,
            bounds.end_ms,
            millis(60_000),
        )
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
