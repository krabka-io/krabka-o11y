use super::*;

#[derive(bon::Builder)]
struct MetricSampleCase {
    group: &'static str,
    instance: &'static str,
    timestamp_ms: i64,
    value: f64,
}

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
    struct ExpectedGroup {
        label: &'static str,
        value: Option<f64>,
    }
    struct AggregationCase {
        query: &'static str,
        expected: &'static [ExpectedGroup],
    }
    macro_rules! expected_groups {
        ($($label:literal => $value:expr),* $(,)?) => {
            &[$(ExpectedGroup { label: $label, value: $value }),*]
        };
    }

    let mut store = InMemoryMetricStore::new();
    // `g` exercises every NaN/stale shape across DISTINCT `by (l)` groups:
    //   l=a: normal finite            -> group value 1.0
    //   l=b: a LONE genuine NaN       -> group KEPT with value NaN
    //   l=c: normal finite            -> group value 3.0
    //   l=d: latest is a STALE marker -> series dropped -> group ABSENT
    //   l=e: a group MIXING genuine NaN with finite {NaN, 2.0, 5.0}
    //        -> sum/avg NaN; min=2/max=5 (NaN ignored); count=3; group=1
    for case in [
        MetricSampleCase::builder()
            .group("a")
            .instance("0")
            .timestamp_ms(120_000)
            .value(1.0)
            .build(),
        MetricSampleCase::builder()
            .group("b")
            .instance("0")
            .timestamp_ms(120_000)
            .value(f64::NAN)
            .build(),
        MetricSampleCase::builder()
            .group("c")
            .instance("0")
            .timestamp_ms(120_000)
            .value(3.0)
            .build(),
        MetricSampleCase::builder()
            .group("d")
            .instance("0")
            .timestamp_ms(60_000)
            .value(7.0)
            .build(),
        MetricSampleCase::builder()
            .group("d")
            .instance("0")
            .timestamp_ms(120_000)
            .value(stale_nan())
            .build(),
        MetricSampleCase::builder()
            .group("e")
            .instance("0")
            .timestamp_ms(120_000)
            .value(f64::NAN)
            .build(),
        MetricSampleCase::builder()
            .group("e")
            .instance("1")
            .timestamp_ms(120_000)
            .value(2.0)
            .build(),
        MetricSampleCase::builder()
            .group("e")
            .instance("2")
            .timestamp_ms(120_000)
            .value(5.0)
            .build(),
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "g"),
            crate::test_support::TestLabelPair::new("l", case.group),
            crate::test_support::TestLabelPair::new("instance", case.instance),
        ]);
        store.push_float("t", lbls, case.timestamp_ms, case.value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Each op must (a) route through the planner, (b) match the interpreter
    // byte-for-byte (NaN-aware), and (c) hit the documented absolute outcome.
    // `expect` maps l -> Some(value) for present groups; l=d is always absent.
    let cases = [
        AggregationCase {
            query: "sum by (l) (g)",
            expected: expected_groups!["a" => Some(1.0), "b" => Some(f64::NAN), "c" => Some(3.0), "e" => Some(f64::NAN)],
        },
        AggregationCase {
            query: "avg by (l) (g)",
            expected: expected_groups!["a" => Some(1.0), "b" => Some(f64::NAN), "c" => Some(3.0), "e" => Some(f64::NAN)],
        },
        AggregationCase {
            query: "min by (l) (g)",
            expected: expected_groups!["a" => Some(1.0), "b" => Some(f64::NAN), "c" => Some(3.0), "e" => Some(2.0)],
        },
        AggregationCase {
            query: "max by (l) (g)",
            expected: expected_groups!["a" => Some(1.0), "b" => Some(f64::NAN), "c" => Some(3.0), "e" => Some(5.0)],
        },
        AggregationCase {
            query: "count by (l) (g)",
            expected: expected_groups!["a" => Some(1.0), "b" => Some(1.0), "c" => Some(1.0), "e" => Some(3.0)],
        },
        AggregationCase {
            query: "group by (l) (g)",
            expected: expected_groups!["a" => Some(1.0), "b" => Some(1.0), "c" => Some(1.0), "e" => Some(1.0)],
        },
        // `without (instance)` groups by `l` (and drops `__name__`): same shape.
        AggregationCase {
            query: "sum without (instance) (g)",
            expected: expected_groups!["a" => Some(1.0), "b" => Some(f64::NAN), "c" => Some(3.0), "e" => Some(f64::NAN)],
        },
    ];

    for case in cases {
        let query = case.query;
        let time_ms = 120_000_i64;
        let parity = planned_and_interpreted(&engine, query, time_ms).await;
        let via_operators = parity.via_operators;
        let via_interpreter = parity.via_interpreter;
        let norm = |result: QueryResult| fingerprint_sorted(result, query);
        let oper = norm(via_operators);
        let interp = norm(via_interpreter);
        assert2::assert!(instant_samples_match(&interp, &oper));
        // The stale group `l=d` is always absent on both paths.
        assert2::assert!(!oper.iter().any(|s| s.labels.get("l") == Some("d")));
        // Absolute Prometheus outcome per group.
        for expected in case.expected {
            let got = oper
                .iter()
                .find(|sample| sample.labels.get("l") == Some(expected.label));
            match expected.value {
                Some(value) => {
                    let sample = got.unwrap_or_else(|| {
                        panic!("`{query}`: group l={} missing in {oper:?}", expected.label)
                    });
                    let got_value = float_value(&sample.value);
                    if value.is_nan() {
                        assert2::assert!(
                            got_value.is_nan() && !super::super::is_stale_nan(got_value)
                        );
                    } else {
                        assert2::assert!(approx_eq(got_value, value));
                    }
                }
                None => assert2::assert!(got.is_none()),
            }
        }
    }
}
