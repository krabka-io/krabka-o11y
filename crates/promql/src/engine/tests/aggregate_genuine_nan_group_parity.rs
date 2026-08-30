use super::*;

/// Differential parity for a genuine NaN series alone in its own `by` group.
///
/// The inner bare selector of a simple aggregation holds one genuine, non-stale
/// NaN series alone in its own group. The operator path must not drop that
/// group. Collapsed `sum(nan_metric)` already pins genuine-NaN propagation, but
/// a selector that drops NaN omits a whole group row here and does not emit the
/// row with the value NaN. This test covers all six simple ops, `by` and
/// `without`, a stale group that the engine drops, and a mixed NaN and finite
/// group where `min` and `max` ignore the NaN. It compares the operator path
/// against the interpreter and asserts the absolute Prometheus outcomes.
#[tokio::test]
pub(crate) async fn aggregate_genuine_nan_group_parity() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    type ExpectedGroup = (&'static str, Option<f64>);
    type AggregationCase = (&'static str, &'static [ExpectedGroup]);

    let mut store = InMemoryMetricStore::new();
    // `g` exercises every NaN/stale shape across DISTINCT `by (l)` groups:
    //   l=a: normal finite            -> group value 1.0
    //   l=b: a LONE genuine NaN       -> group KEPT with value NaN
    //   l=c: normal finite            -> group value 3.0
    //   l=d: latest is a STALE marker -> series dropped -> group ABSENT
    //   l=e: a group MIXING genuine NaN with finite {NaN, 2.0, 5.0}
    //        -> sum/avg NaN; min=2/max=5 (NaN ignored); count=3; group=1
    for (l, instance, ts, value) in [
        ("a", "0", 120_000_i64, 1.0_f64),
        ("b", "0", 120_000, f64::NAN),
        ("c", "0", 120_000, 3.0),
        ("d", "0", 60_000, 7.0),
        ("d", "0", 120_000, stale_nan()),
        ("e", "0", 120_000, f64::NAN),
        ("e", "1", 120_000, 2.0),
        ("e", "2", 120_000, 5.0),
    ] {
        let lbls = labels(&[("__name__", "g"), ("l", l), ("instance", instance)]);
        store.push_float("t", lbls, ts, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Each op must (a) route through the planner, (b) match the interpreter
    // byte-for-byte (NaN-aware), and (c) hit the documented absolute outcome.
    // `expect` maps l -> Some(value) for present groups; l=d is always absent.
    let cases: &[AggregationCase] = &[
        (
            "sum by (l) (g)",
            &[
                ("a", Some(1.0)),
                ("b", Some(f64::NAN)),
                ("c", Some(3.0)),
                ("e", Some(f64::NAN)),
            ],
        ),
        (
            "avg by (l) (g)",
            &[
                ("a", Some(1.0)),
                ("b", Some(f64::NAN)),
                ("c", Some(3.0)),
                ("e", Some(f64::NAN)),
            ],
        ),
        (
            "min by (l) (g)",
            &[
                ("a", Some(1.0)),
                ("b", Some(f64::NAN)),
                ("c", Some(3.0)),
                ("e", Some(2.0)),
            ],
        ),
        (
            "max by (l) (g)",
            &[
                ("a", Some(1.0)),
                ("b", Some(f64::NAN)),
                ("c", Some(3.0)),
                ("e", Some(5.0)),
            ],
        ),
        (
            "count by (l) (g)",
            &[
                ("a", Some(1.0)),
                ("b", Some(1.0)),
                ("c", Some(1.0)),
                ("e", Some(3.0)),
            ],
        ),
        (
            "group by (l) (g)",
            &[
                ("a", Some(1.0)),
                ("b", Some(1.0)),
                ("c", Some(1.0)),
                ("e", Some(1.0)),
            ],
        ),
        // `without (instance)` groups by `l` (and drops `__name__`): same shape.
        (
            "sum without (instance) (g)",
            &[
                ("a", Some(1.0)),
                ("b", Some(f64::NAN)),
                ("c", Some(3.0)),
                ("e", Some(f64::NAN)),
            ],
        ),
    ];

    for (query, expect) in cases {
        let time_ms = 120_000_i64;
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
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
        let norm = |r: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut s) = r else {
                panic!("expected vector for `{query}`");
            };
            s.sort_by_key(|item| item.labels.fingerprint());
            s
        };
        let oper = norm(via_operators);
        let interp = norm(via_interpreter);
        assert2::assert!(instant_samples_match(&interp, &oper));
        // The stale group `l=d` is always absent on both paths.
        assert2::assert!(!oper.iter().any(|s| s.labels.get("l") == Some("d")));
        // Absolute Prometheus outcome per group.
        for (l, want) in *expect {
            let got = oper.iter().find(|s| s.labels.get("l") == Some(*l));
            match want {
                Some(value) => {
                    let sample =
                        got.unwrap_or_else(|| panic!("`{query}`: group l={l} missing in {oper:?}"));
                    let got_value = float_value(&sample.value);
                    if value.is_nan() {
                        assert2::assert!(
                            got_value.is_nan() && !super::super::is_stale_nan(got_value)
                        );
                    } else {
                        assert2::assert!(approx_eq(got_value, *value));
                    }
                }
                None => assert2::assert!(got.is_none()),
            }
        }
    }
}
