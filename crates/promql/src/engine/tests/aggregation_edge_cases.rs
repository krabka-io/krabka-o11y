use super::*;

mod aggregate_genuine_nan_group_parity {
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
                                got_value.is_nan() && !super::super::super::is_stale_nan(got_value)
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
}

mod avg_over_time_carries_infinities_and_keeps_its_compensation {
    use super::*;

    #[derive(bon::Builder)]
    struct SeriesCase {
        name: &'static str,
        values: Vec<f64>,
    }

    struct ExpectedMeanCase {
        name: &'static str,
        expected: Option<f64>,
    }

    /// `avg_over_time` carries infinities the way Prometheus does: a mean that has
    /// gone infinite stays there through finite samples, an infinity of the same
    /// sign keeps it, and only the opposite infinity or a NaN turns it into a NaN.
    /// Nothing fed the fold an infinity before, which left every arm of that guard
    /// free -- and hid that the Kahan compensation went NaN on the very first
    /// infinite increment and rode all the way out to the result.
    ///
    /// The last case pins the compensation itself. `1.0 + 0.1 + 0.1` sums to
    /// `1.2000000000000002` without it, one ulp off the `1.2` that the compensated
    /// sum reaches, so the mean differs in its final bit. It is compared
    /// bit-for-bit; a relative tolerance would accept the fold running backwards.
    #[tokio::test]
    pub(crate) async fn avg_over_time_carries_infinities_and_keeps_its_compensation() {
        let mut store = InMemoryMetricStore::new();
        let series = [
            SeriesCase::builder()
                .name("inf_then_finite")
                .values(vec![f64::INFINITY, 1.0, 2.0])
                .build(),
            SeriesCase::builder()
                .name("neg_inf_then_finite")
                .values(vec![f64::NEG_INFINITY, 1.0])
                .build(),
            SeriesCase::builder()
                .name("opposite_infinities")
                .values(vec![f64::INFINITY, f64::NEG_INFINITY])
                .build(),
            SeriesCase::builder()
                .name("inf_then_nan")
                .values(vec![f64::INFINITY, f64::NAN])
                .build(),
            SeriesCase::builder()
                .name("inf_twice")
                .values(vec![f64::INFINITY, f64::INFINITY])
                .build(),
            SeriesCase::builder()
                .name("finite_then_inf")
                .values(vec![1.0, f64::INFINITY])
                .build(),
            SeriesCase::builder()
                .name("compensated")
                .values(vec![1.0, 0.1, 0.1])
                .build(),
        ];
        for case in &series {
            for index in 0..case.values.len() {
                let ts_ms = 10_000 + 10_000 * i64::try_from(index).expect("a small index");
                store.push_float(
                    "tenant-a",
                    labels(&[
                        crate::test_support::TestLabelPair::new("__name__", case.name),
                        crate::test_support::TestLabelPair::new("series", "f"),
                    ]),
                    ts_ms,
                    case.values[index],
                );
            }
            // `*_over_time` lowers onto a float-only operator leaf unless the
            // selector also matches a native histogram, which routes the call to
            // the interpreter kernel's own fold. Without a sibling here the kernel
            // copy is never executed.
            store.push_histogram(
                "tenant-a",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", case.name),
                    crate::test_support::TestLabelPair::new("series", "h"),
                ]),
                20_000,
                native_histogram(4.0, 10.0),
            );
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        for case in [
            // A finite sample cannot pull an infinite mean back.
            ExpectedMeanCase {
                name: "inf_then_finite",
                expected: Some(f64::INFINITY),
            },
            ExpectedMeanCase {
                name: "neg_inf_then_finite",
                expected: Some(f64::NEG_INFINITY),
            },
            // Opposite infinities, and a NaN, are the two things that do.
            ExpectedMeanCase {
                name: "opposite_infinities",
                expected: None,
            },
            ExpectedMeanCase {
                name: "inf_then_nan",
                expected: None,
            },
            // Same-sign infinity leaves it alone, and an infinity arriving after a
            // finite start still takes the mean there.
            ExpectedMeanCase {
                name: "inf_twice",
                expected: Some(f64::INFINITY),
            },
            ExpectedMeanCase {
                name: "finite_then_inf",
                expected: Some(f64::INFINITY),
            },
        ] {
            let query = format!("avg_over_time({}[1m])", case.name);
            let QueryResult::InstantVector(samples) = engine
                .query_instant(&tenant_id("tenant-a"), &query, 60_000)
                .await
                .unwrap_or_else(|error| panic!("{query}: {error}"))
            else {
                panic!("expected a vector for {query}");
            };
            let float = samples
                .iter()
                .find(|sample| sample.labels.get("series") == Some("f"))
                .unwrap_or_else(|| panic!("{query}: no float series in the result"));
            let got = float_value(&float.value);
            match case.expected {
                Some(want) => {
                    assert2::assert!(got.to_bits() == want.to_bits(), "{query}: got {got}");
                }
                None => assert2::assert!(got.is_nan(), "{query}: got {got}"),
            }
        }

        let QueryResult::InstantVector(samples) = engine
            .query_instant(
                &tenant_id("tenant-a"),
                "avg_over_time(compensated[1m])",
                60_000,
            )
            .await
            .expect("a compensated mean")
        else {
            panic!("expected a vector");
        };
        let float = sample_with_label(&samples, "series", "f");
        assert2::assert!(
            float_value(&float.value).to_bits() == 0.4_f64.to_bits(),
            "the Kahan compensation is added, not subtracted or dropped"
        );
    }
}

mod kahan_sum_inc_recovers_lost_bits_on_both_branches {
    use super::*;
    /// `kahan_sum_inc` recovers the bits lost when one operand dwarfs the other,
    /// on both magnitude branches, and gives up on an infinite running sum rather
    /// than carrying a NaN forward.
    #[test]
    pub(crate) fn kahan_sum_inc_recovers_lost_bits_on_both_branches() {
        use super::super::super::range_functions::kahan_sum_inc;

        // |sum| >= |increment|: the increment falls off the end of the sum, and
        // all of it comes back as compensation.
        let mut sum = 1e16;
        let mut comp = 0.0;
        kahan_sum_inc(1.0, &mut sum, &mut comp);
        assert2::assert!(sum.to_bits() == 1e16_f64.to_bits());
        assert2::assert!(comp.to_bits() == 1.0_f64.to_bits(), "got {comp}");

        // The swapped branch has to compute the residue the other way round.
        let mut sum = 1.0;
        let mut comp = 0.0;
        kahan_sum_inc(1e16, &mut sum, &mut comp);
        assert2::assert!(sum.to_bits() == 1e16_f64.to_bits());
        assert2::assert!(comp.to_bits() == 1.0_f64.to_bits(), "got {comp}");

        // An infinite sum leaves no residue to recover; computing one gives NaN.
        let mut sum = 0.0;
        let mut comp = 0.0;
        kahan_sum_inc(f64::INFINITY, &mut sum, &mut comp);
        assert2::assert!(sum.to_bits() == f64::INFINITY.to_bits());
        assert2::assert!(comp.to_bits() == 0.0_f64.to_bits(), "got {comp}");
    }
}

mod sum_avg_collapsed_is_deterministic_and_matches_interpreter {
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
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
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
}
