use super::*;

mod histogram_aggregation_planner_path_matches_interpreter {
    use super::*;

    fn group_labels(group: &str, instance: &str) -> Labels {
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "m"),
            crate::test_support::TestLabelPair::new("g", group),
            crate::test_support::TestLabelPair::new("instance", instance),
        ])
    }

    #[derive(bon::Builder)]
    struct FloatAggregationSample {
        instance: &'static str,
        value: f64,
    }

    struct AggregationQueryCase {
        query: &'static str,
        timestamp_ms: i64,
    }

    /// Differential parity for histogram-bearing aggregations in the planner.
    ///
    /// The recursive planner routes these aggregations through the shared
    /// `apply_simple_aggregate` and `apply_*` kernels, which is the `Precomputed`
    /// path. Each query MUST claim the operator path and MUST match the interpreter
    /// byte-for-byte. The match covers the native-histogram payloads, which the
    /// test compares structurally and not by float `==`, and any warning or info
    /// annotation.
    ///
    /// The store exercises every native-histogram aggregation rule:
    /// - `sum`/`avg` merge compatible histograms, and `avg` scales by `1/count`;
    /// - `sum`/`avg` drop a group that mixes a float and a histogram, which is the
    ///   mixed-sample rule;
    /// - `count`/`group` count every sample regardless of type;
    /// - `min`/`max`/`stddev`/`stdvar`/`topk`/`bottomk`/`quantile` ignore and drop
    ///   histogram samples, and reduce only the floats;
    /// - `count_values` formats a histogram value as Prometheus bucket notation.
    #[tokio::test]
    pub(crate) async fn histogram_aggregation_planner_path_matches_interpreter() {
        use crate::{DurationExprContext, parse_promql_with_duration_context};

        let mut store = InMemoryMetricStore::new();
        // Group g="hist": TWO compatible native histograms (so sum/avg actually
        // merge), across an `instance` dimension so `without (instance)` collapses
        // them into one group.
        store.push_histogram(
            "t",
            group_labels("hist", "0"),
            300_000,
            two_positive_bucket_histogram(4.0, 6.0),
        );
        store.push_histogram(
            "t",
            group_labels("hist", "1"),
            300_000,
            two_positive_bucket_histogram(8.0, 20.0),
        );
        // Group g="float": TWO float members (so the float aggregations reduce a
        // real group and `count`/`group` see floats).
        for sample in [
            FloatAggregationSample::builder()
                .instance("0")
                .value(2.0)
                .build(),
            FloatAggregationSample::builder()
                .instance("1")
                .value(6.0)
                .build(),
        ] {
            store.push_float(
                "t",
                group_labels("float", sample.instance),
                300_000,
                sample.value,
            );
        }
        // Group g="mixed": ONE float + ONE histogram in the same group. Under
        // `sum`/`avg` this group is dropped (mixed-sample rule); under
        // `count`/`group` it counts 2; under the histogram-ignoring ops only the
        // float survives.
        store.push_float("t", group_labels("mixed", "0"), 300_000, 10.0);
        store.push_histogram(
            "t",
            group_labels("mixed", "1"),
            300_000,
            two_positive_bucket_histogram(2.0, 3.0),
        );
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let queries = [
            // sum/avg MERGE histograms per group; the mixed group is dropped.
            AggregationQueryCase {
                query: "sum by (g) (m)",
                timestamp_ms: 300_000,
            },
            AggregationQueryCase {
                query: "avg by (g) (m)",
                timestamp_ms: 300_000,
            },
            AggregationQueryCase {
                query: "sum without (instance) (m)",
                timestamp_ms: 300_000,
            },
            AggregationQueryCase {
                query: "avg without (instance) (m)",
                timestamp_ms: 300_000,
            },
            // Global sum over everything: the lone global group mixes floats and
            // histograms, so it is dropped entirely (empty result).
            AggregationQueryCase {
                query: "sum(m)",
                timestamp_ms: 300_000,
            },
            // count/group count every sample regardless of type.
            AggregationQueryCase {
                query: "count by (g) (m)",
                timestamp_ms: 300_000,
            },
            AggregationQueryCase {
                query: "group by (g) (m)",
                timestamp_ms: 300_000,
            },
            AggregationQueryCase {
                query: "count without (instance) (m)",
                timestamp_ms: 300_000,
            },
            AggregationQueryCase {
                query: "count(m)",
                timestamp_ms: 300_000,
            },
            // min/max/stddev/stdvar IGNORE histograms (reduce only floats); the
            // all-histogram g="hist" group produces no row, g="float" reduces its
            // two floats, g="mixed" reduces just its one float.
            AggregationQueryCase {
                query: "min by (g) (m)",
                timestamp_ms: 300_000,
            },
            AggregationQueryCase {
                query: "max by (g) (m)",
                timestamp_ms: 300_000,
            },
            AggregationQueryCase {
                query: "stddev by (g) (m)",
                timestamp_ms: 300_000,
            },
            AggregationQueryCase {
                query: "stdvar by (g) (m)",
                timestamp_ms: 300_000,
            },
            // topk/bottomk/quantile also IGNORE histograms.
            AggregationQueryCase {
                query: "topk by (g) (1, m)",
                timestamp_ms: 300_000,
            },
            AggregationQueryCase {
                query: "bottomk by (g) (1, m)",
                timestamp_ms: 300_000,
            },
            AggregationQueryCase {
                query: "quantile by (g) (0.5, m)",
                timestamp_ms: 300_000,
            },
            // count_values formats histogram values as Prometheus bucket notation.
            AggregationQueryCase {
                query: "count_values by (g) (\"v\", m)",
                timestamp_ms: 300_000,
            },
        ];

        for case in queries {
            assert_annotated_call_planner_matches_interpreter(
                &engine,
                case.query,
                case.timestamp_ms,
            )
            .await;
        }

        // Pin the absolute histogram-aware rules (not just operator==interpreter).
        let sample_by_group =
            |samples: &[crate::InstantSample], g: &str| -> Option<crate::InstantSample> {
                samples
                    .iter()
                    .find(|sample| sample.labels.get("g") == Some(g))
                    .cloned()
            };

        // `sum by (g) (m)`: g="hist" is the MERGED histogram (count 4+8=12,
        // sum 6+20=26), g="float" sums its two floats (2+6=8), g="mixed" is
        // DROPPED (float+histogram).
        let sum_expr = parse_promql_with_duration_context(
            "sum by (g) (m)",
            DurationExprContext::instant(300_000),
        )
        .expect("parse sum");
        let plan = engine
            .plan_instant_expr("t", &sum_expr, 300_000)
            .await
            .expect("plan sum")
            .expect("sum routes through planner");
        let QueryResult::InstantVector(sum_samples) = engine
            .assemble_planned_instant(plan, 300_000)
            .await
            .expect("assemble sum")
        else {
            panic!("expected vector for sum");
        };
        assert2::assert!(sample_by_group(&sum_samples, "mixed").is_none());
        let hist_row = sample_by_group(&sum_samples, "hist").expect("sum: g=hist row");
        let SampleValue::Histogram(merged) = hist_row.value else {
            panic!("sum: g=hist must be a merged histogram, got: {hist_row:?}");
        };
        assert2::assert!(approx_eq(merged.count, 12.0) && approx_eq(merged.sum, 26.0));
        let float_row = sample_by_group(&sum_samples, "float").expect("sum: g=float row");
        assert2::assert!(approx_eq(float_value(&float_row.value), 8.0));

        // `min by (g) (m)`: g="hist" (all histograms) yields NO row; g="float"
        // reduces to 2; g="mixed" reduces to just its one float (10).
        let min_expr = parse_promql_with_duration_context(
            "min by (g) (m)",
            DurationExprContext::instant(300_000),
        )
        .expect("parse min");
        let plan = engine
            .plan_instant_expr("t", &min_expr, 300_000)
            .await
            .expect("plan min")
            .expect("min routes through planner");
        let QueryResult::InstantVector(min_samples) = engine
            .assemble_planned_instant(plan, 300_000)
            .await
            .expect("assemble min")
        else {
            panic!("expected vector for min");
        };
        check!(
            sample_by_group(&min_samples, "hist").is_none(),
            "min: all-histogram group must be absent (histograms ignored), got: {min_samples:?}"
        );
        check!(
            approx_eq(
                float_value(
                    &sample_by_group(&min_samples, "float")
                        .expect("min g=float")
                        .value
                ),
                2.0
            ),
            "min: g=float must be 2"
        );
        check!(
            approx_eq(
                float_value(
                    &sample_by_group(&min_samples, "mixed")
                        .expect("min g=mixed")
                        .value
                ),
                10.0
            ),
            "min: g=mixed must reduce just its float (10)"
        );
    }
}

mod histogram_float_comparison_emits_incompatible_types_info {
    use super::*;

    #[tokio::test]
    pub(crate) async fn histogram_float_comparison_emits_incompatible_types_info() {
        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "h"),
                crate::test_support::TestLabelPair::new("job", "app"),
            ]),
            0,
            native_histogram(4.0, 5.0),
        );
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let annotated = engine
            .query_instant_with_annotations(&tenant_id("tenant-a"), "h > 80", 0)
            .await
            .expect("query");
        assert2::assert!(
            matches!(annotated.result, QueryResult::InstantVector(ref v) if v.is_empty())
        );
        assert2::assert!(annotated.annotations == crate::Annotations {
            infos: vec![
                "PromQL info: incompatible sample types encountered for binary operator \">\": histogram > float (1:1)"
                    .to_string()
            ],
            warnings: vec![],
            ..crate::Annotations::default()
        });
    }
}

mod histogram_fraction_counts_open_ended_classic_buckets_only_against_open_bounds {
    use super::*;

    #[derive(bon::Builder)]
    struct ClassicBucket {
        upper_bound: &'static str,
        cumulative_count: f64,
    }

    struct ClassicHistogramFixture {
        name: &'static str,
        buckets: Vec<ClassicBucket>,
    }

    struct HistogramFractionCase {
        query: &'static str,
        expected: f64,
    }

    /// `histogram_fraction` over classic buckets, where the outermost bounds are
    /// infinite. An open-ended bucket counts in full only when the query's own
    /// bound is open the same way -- a finite bound never reaches into it, however
    /// far out it goes. Nothing exercised those two arms, nor a query bound that
    /// lands inside a bucket rather than on its edge.
    #[tokio::test]
    pub(crate) async fn histogram_fraction_counts_open_ended_classic_buckets_only_against_open_bounds()
     {
        let mut store = InMemoryMetricStore::new();
        // Cumulative counts. The first `le` is negative, so the opening bucket runs
        // from -Inf, and the `+Inf` bucket closes the series at the other end.
        for histogram in [
            ClassicHistogramFixture {
                name: "hc",
                buckets: vec![
                    ClassicBucket::builder()
                        .upper_bound("-1")
                        .cumulative_count(2.0)
                        .build(),
                    ClassicBucket::builder()
                        .upper_bound("0")
                        .cumulative_count(3.0)
                        .build(),
                    ClassicBucket::builder()
                        .upper_bound("1")
                        .cumulative_count(6.0)
                        .build(),
                    ClassicBucket::builder()
                        .upper_bound("+Inf")
                        .cumulative_count(10.0)
                        .build(),
                ],
            },
            ClassicHistogramFixture {
                name: "hc2",
                buckets: vec![
                    ClassicBucket::builder()
                        .upper_bound("-1")
                        .cumulative_count(2.0)
                        .build(),
                    ClassicBucket::builder()
                        .upper_bound("+Inf")
                        .cumulative_count(5.0)
                        .build(),
                ],
            },
            // A bucket four wide, so dividing by its width is not the same as
            // multiplying by it -- which it is for every bucket above.
            ClassicHistogramFixture {
                name: "hc3",
                buckets: vec![
                    ClassicBucket::builder()
                        .upper_bound("0")
                        .cumulative_count(0.0)
                        .build(),
                    ClassicBucket::builder()
                        .upper_bound("4")
                        .cumulative_count(8.0)
                        .build(),
                    ClassicBucket::builder()
                        .upper_bound("+Inf")
                        .cumulative_count(10.0)
                        .build(),
                ],
            },
        ] {
            for bucket in histogram.buckets {
                store.push_float(
                    "tenant-a",
                    labels(&[
                        crate::test_support::TestLabelPair::new("__name__", histogram.name),
                        crate::test_support::TestLabelPair::new("le", bucket.upper_bound),
                    ]),
                    10_000,
                    bucket.cumulative_count,
                );
            }
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        for case in [
            // A range on bucket edges takes whole buckets.
            HistogramFractionCase {
                query: "histogram_fraction(0, 1, hc)",
                expected: 0.3,
            },
            // The -Inf bucket counts only against an -Inf bound...
            HistogramFractionCase {
                query: "histogram_fraction(-Inf, -1, hc)",
                expected: 0.2,
            },
            HistogramFractionCase {
                query: "histogram_fraction(-2, -1, hc)",
                expected: 0.0,
            },
            // ...and the +Inf bucket only against a +Inf bound.
            HistogramFractionCase {
                query: "histogram_fraction(1, Inf, hc)",
                expected: 0.4,
            },
            HistogramFractionCase {
                query: "histogram_fraction(1, 2, hc)",
                expected: 0.0,
            },
            // A bucket open at the top with a finite lower edge is still reached
            // through the upper arm, not the lower one.
            HistogramFractionCase {
                query: "histogram_fraction(-1, Inf, hc2)",
                expected: 0.6,
            },
            // Everything, and a bound landing inside a bucket rather than on it.
            HistogramFractionCase {
                query: "histogram_fraction(-Inf, Inf, hc)",
                expected: 1.0,
            },
            HistogramFractionCase {
                query: "histogram_fraction(0.5, 1, hc)",
                expected: 0.15,
            },
            // Partial cover of a four-wide bucket: half of it, not eight times it.
            HistogramFractionCase {
                query: "histogram_fraction(1, 3, hc3)",
                expected: 0.4,
            },
            HistogramFractionCase {
                query: "histogram_fraction(0, 4, hc3)",
                expected: 0.8,
            },
            HistogramFractionCase {
                query: "histogram_fraction(2, Inf, hc3)",
                expected: 0.6,
            },
        ] {
            assert_lone_value(&engine, case.query, case.expected).await;
        }
    }
}

mod histogram_fraction_mixed_emits_exact_warning {
    use super::*;

    #[tokio::test]
    pub(crate) async fn histogram_fraction_mixed_emits_exact_warning() {
        let engine = PromqlEngine::new(Arc::new(mixed_histogram_store()), EngineOpts::default());
        let annotated = engine
            .query_instant_with_annotations(
                &tenant_id("tenant-a"),
                "histogram_fraction(-Inf, 1, series)",
                0,
            )
            .await
            .expect("query");
        let annotations = annotated.annotations;
        assert2::assert!(
        annotations.warnings.iter().any(|w| w
            == "PromQL warning: vector contains a mix of classic and native histograms for metric name \"series\" (1:29)")
    );
    }
}

mod histogram_quantile_interpolates_each_kind_of_native_bucket {
    use super::*;

    struct QuantileExpectation {
        quantile: f64,
        value: f64,
    }

    /// `histogram_quantile` interpolates inside the bucket the rank lands in, and
    /// which way depends on where that bucket sits. One spanning zero interpolates
    /// linearly; one entirely positive interpolates geometrically; one entirely
    /// negative does the same on magnitudes and flips the sign back. Only the
    /// linear arm had ever run -- the negative arm's sign, its multiply and its
    /// ratio were all free, and in a bucket starting at 1.0 dividing by the lower
    /// bound is the same as multiplying by it, so the positive arm needs a bucket
    /// that starts somewhere else.
    ///
    /// The quantiles below the median are found by walking up from the smallest
    /// bucket, and those at or above it by walking down from the largest, as
    /// Prometheus does, so the set spans both directions.
    #[tokio::test]
    pub(crate) async fn histogram_quantile_interpolates_each_kind_of_native_bucket() {
        // Buckets -4..-2 and -2..-1, then the zero bucket, then 1..2 and 2..4.
        // The last is the one that separates dividing by the lower bound from
        // multiplying by it.
        let engine = signed_bucket_histogram_engine(0.0);

        for case in [
            // Lands in -2..-1: geometric on magnitudes, negated.
            QuantileExpectation {
                quantile: 0.1,
                value: -1.909_683_207_820_833,
            },
            // Lands inside the zero bucket, not on an edge -- at the edge the
            // interpolation returns the upper bound whatever the lower one is.
            QuantileExpectation {
                quantile: 0.35,
                value: -0.400_000_000_000_000_36,
            },
            // At the median the walk turns around and comes down from the top
            // bucket instead, which lands it on the lower bound of `1..2`.
            QuantileExpectation {
                quantile: 0.5,
                value: 1.0,
            },
            // Lands in 1..2 and then in 2..4: geometric.
            QuantileExpectation {
                quantile: 0.75,
                value: 1.681_792_830_507_429,
            },
            QuantileExpectation {
                quantile: 0.9,
                value: 2.639_015_821_545_789_3,
            },
        ] {
            let query = format!("histogram_quantile({}, h)", case.quantile);
            assert_lone_value(&engine, &query, case.value).await;
        }
    }
}

mod histogram_quantile_mixed_emits_exact_warning_and_no_info {
    use super::*;

    #[tokio::test]
    pub(crate) async fn histogram_quantile_mixed_emits_exact_warning_and_no_info() {
        let engine = PromqlEngine::new(Arc::new(mixed_histogram_store()), EngineOpts::default());
        let annotated = engine
            .query_instant_with_annotations(
                &tenant_id("tenant-a"),
                "histogram_quantile(0.8, series)",
                0,
            )
            .await
            .expect("query");
        let annotations = annotated.annotations;
        assert2::assert!(
        annotations
            == crate::Annotations {
                warnings: vec![
                    "PromQL warning: vector contains a mix of classic and native histograms for metric name \"series\" (1:25)"
                        .to_string()
                ],
                infos: vec![],
                ..crate::Annotations::default()
            }
    );
    }
}

mod histogram_quantiles_emits_one_sample_per_requested_quantile {
    #[cfg(feature = "experimental-functions")]
    use super::*;

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    pub(crate) async fn histogram_quantiles_emits_one_sample_per_requested_quantile() {
        let engine = PromqlEngine::new(Arc::new(classic_bucket_store()), EngineOpts::default());
        let result = engine
        .query_instant(
            &tenant_id("tenant-a"),
            r#"histogram_quantiles(http_request_duration_seconds_bucket, "quantile", 0.5, 0.9)"#,
            10_000,
        )
        .await
        .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert2::assert!(samples.len() == 2);
        let mut values = BTreeMap::new();
        for sample in &samples {
            values.insert(
                sample.labels.get("quantile").expect("quantile label"),
                float_value(&sample.value),
            );
        }
        check!(approx_eq(*values.get("0.5").expect("p50 sample"), 0.25));
        check!(approx_eq(*values.get("0.9").expect("p90 sample"), 0.37));
        check!(samples.iter().all(|sample| {
            sample.labels.get("__name__").is_none()
                && sample.labels.get("job") == Some("api")
                && sample.labels.get("le").is_none()
        }));
    }
}

mod histogram_quantiles_requires_experimental_feature {
    #[cfg(not(feature = "experimental-functions"))]
    use super::*;

    #[cfg(not(feature = "experimental-functions"))]
    #[tokio::test]
    pub(crate) async fn histogram_quantiles_requires_experimental_feature() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
        let error = engine
            .query_instant(
                &tenant_id("tenant-a"),
                r#"histogram_quantiles(vector(1), "quantile", 0.5)"#,
                10_000,
            )
            .await
            .unwrap_err();

        assert2::assert!(format!("{error}").contains("experimental-functions"));
    }
}

mod histogram_range_planner_path_matches_interpreter {
    use super::*;

    struct CounterHistogramSample {
        timestamp_ms: i64,
        count: f64,
        sum: f64,
        first_bucket: f64,
        second_bucket: f64,
    }

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
        let series = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "h"),
            crate::test_support::TestLabelPair::new("job", "api"),
        ]);
        for sample in [
            CounterHistogramSample {
                timestamp_ms: 60_000,
                count: 4.0,
                sum: 6.0,
                first_bucket: 1.0,
                second_bucket: 3.0,
            },
            CounterHistogramSample {
                timestamp_ms: 120_000,
                count: 6.0,
                sum: 10.0,
                first_bucket: 2.0,
                second_bucket: 4.0,
            },
            CounterHistogramSample {
                timestamp_ms: 180_000,
                count: 9.0,
                sum: 15.0,
                first_bucket: 3.0,
                second_bucket: 6.0,
            },
            // COUNTER RESET: every component decreases below the prior sample.
            CounterHistogramSample {
                timestamp_ms: 240_000,
                count: 2.0,
                sum: 3.0,
                first_bucket: 1.0,
                second_bucket: 1.0,
            },
            CounterHistogramSample {
                timestamp_ms: 300_000,
                count: 5.0,
                sum: 8.0,
                first_bucket: 2.0,
                second_bucket: 3.0,
            },
        ] {
            store.push_histogram(
                "t",
                series.clone(),
                sample.timestamp_ms,
                counter_histogram(
                    sample.count,
                    sample.sum,
                    sample.first_bucket,
                    sample.second_bucket,
                ),
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
            assert_annotated_call_planner_matches_interpreter(&engine, query, time_ms).await;
        }

        // Pin the absolute rules the parity above relies on (not just
        // operator==interpreter).

        // `rate(h[10m])` yields one histogram sample whose name is pending removal
        // at the public query boundary.
        let rate_expr = parse_promql_with_duration_context(
            "rate(h[10m])",
            DurationExprContext::instant(time_ms),
        )
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
        assert2::assert!(rate_samples[0].labels.get("__name__") == Some("h"));
        assert2::assert!(rate_samples[0].drop_name);
        assert2::assert!(rate_samples[0].drop_name);
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
}

mod histogram_stdvar_places_each_bucket_at_its_own_midpoint {
    use super::*;

    /// `histogram_stdvar` weights each bucket by the square of the distance from
    /// its representative value to the mean, and that value is the geometric
    /// midpoint -- negated for a bucket below zero, arithmetic for one spanning
    /// it. The `sum` here is deliberately not zero: at a mean of zero the squaring
    /// hides the sign, so a negative bucket's midpoint could come back positive
    /// and the variance would not move.
    #[tokio::test]
    pub(crate) async fn histogram_stdvar_places_each_bucket_at_its_own_midpoint() {
        let engine = signed_bucket_histogram_engine(12.0);

        for case in [
            QueryValueCase {
                query: "histogram_stdvar(h)",
                value: 3.459_559_885_480_119_5,
            },
            QueryValueCase {
                query: "histogram_stddev(h)",
                value: 1.859_989_216_495_654_6,
            },
        ] {
            assert_lone_value(&engine, case.query, case.value).await;
        }
    }
}

mod histogram_stdvar_takes_custom_bucket_bounds_from_get_bound {
    use super::*;

    #[derive(bon::Builder)]
    struct HistogramVarianceCase {
        name: &'static str,
        function: &'static str,
        histogram: NativeHistogram,
        want: f64,
    }

    fn custom_histogram(
        sum: f64,
        custom_values: &[f64],
        offset: i32,
        counts: &[f64],
    ) -> NativeHistogram {
        NativeHistogram {
            schema: -53,
            count: counts.iter().sum(),
            sum,
            positive_spans: vec![BucketSpan {
                offset,
                length: u32::try_from(counts.len()).expect("a short bucket list"),
            }],
            positive_counts: counts.to_vec(),
            custom_values: Some(custom_values.to_vec()),
            ..native_histogram(0.0, 0.0)
        }
    }

    #[tokio::test]
    pub(crate) async fn histogram_stdvar_takes_custom_bucket_bounds_from_get_bound() {
        // Each want is the answer of the Prometheus v3.14.0 engine, through
        // `promqltest` of github.com/prometheus/prometheus v0.314.0.
        let cases = [
            HistogramVarianceCase::builder()
                .name("first bucket is open below")
                .function("histogram_stdvar")
                .histogram(custom_histogram(1.0, &[0.1, 0.5], 0, &[1.0, 1.0]))
                .want(f64::INFINITY)
                .build(),
            HistogramVarianceCase::builder()
                .name("last bucket is open above")
                .function("histogram_stdvar")
                .histogram(custom_histogram(1.0, &[0.1, 0.5], 1, &[1.0, 1.0]))
                .want(f64::INFINITY)
                .build(),
            HistogramVarianceCase::builder()
                .name("closed buckets")
                .function("histogram_stdvar")
                .histogram(custom_histogram(0.5, &[0.1, 0.5, 2.5], 1, &[1.0, 1.0]))
                .want(0.7825)
                .build(),
            HistogramVarianceCase::builder()
                .name("closed buckets")
                .function("histogram_stddev")
                .histogram(custom_histogram(0.5, &[0.1, 0.5, 2.5], 1, &[1.0, 1.0]))
                .want(0.884_590_300_647_706_6)
                .build(),
            HistogramVarianceCase::builder()
                .name("empty open buckets")
                .function("histogram_stdvar")
                .histogram(custom_histogram(
                    0.5,
                    &[0.1, 0.5, 2.5],
                    0,
                    &[0.0, 1.0, 1.0, 0.0],
                ))
                .want(0.7825)
                .build(),
            HistogramVarianceCase::builder()
                .name("negative bounds")
                .function("histogram_stdvar")
                .histogram(custom_histogram(-3.0, &[-4.0, -2.0, 0.0], 1, &[1.0, 1.0]))
                .want(1.25)
                .build(),
            HistogramVarianceCase::builder()
                .name("bucket across zero")
                .function("histogram_stdvar")
                .histogram(custom_histogram(-1.0, &[-1.0, 1.0], 1, &[2.0]))
                .want(0.25)
                .build(),
            HistogramVarianceCase::builder()
                .name("one bucket open at both ends")
                .function("histogram_stdvar")
                .histogram(custom_histogram(1.0, &[], 0, &[1.0]))
                .want(f64::NAN)
                .build(),
        ];

        for case_index in 0..cases.len() {
            let case = &cases[case_index];
            let mut store = InMemoryMetricStore::new();
            store.push_histogram(
                "tenant-a",
                labels(&[crate::test_support::TestLabelPair::new("__name__", "h")]),
                10_000,
                case.histogram.clone(),
            );
            let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
            let query = format!("{}(h)", case.function);

            let result = engine
                .query_instant(&tenant_id("tenant-a"), &query, 10_000)
                .await
                .unwrap_or_else(|error| panic!("{}: {error}", case.name));

            let QueryResult::InstantVector(samples) = result else {
                panic!("{}: expected a vector", case.name);
            };
            check!(samples.len() == 1, "{}", case.name);
            let got = float_value(&samples[0].value);
            // Every NaN matches every other: its sign bit depends on the target
            // CPU, and Prometheus does not compare NaN payloads either.
            check!(
                (got.is_nan() && case.want.is_nan())
                    || got.total_cmp(&case.want).is_eq()
                    || approx_eq(got, case.want),
                "{} {}: {got} != {}",
                case.name,
                case.function,
                case.want
            );
        }
    }
}

mod native_histogram_bucket_bounds_follow_the_schema {
    use super::*;

    /// Bucket bounds at a schema other than zero. At schema 0 the exponent factor
    /// is one, so multiplying the index by it, dividing by it, and negating the
    /// schema all give the same bound -- every histogram test above uses that
    /// schema. At schema 1 the buckets are a square root apart.
    #[tokio::test]
    pub(crate) async fn native_histogram_bucket_bounds_follow_the_schema() {
        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "h")]),
            10_000,
            NativeHistogram {
                schema: 1,
                is_float: true,
                reset_hint: ResetHint::No,
                zero_threshold: 0.0,
                zero_count: 0.0,
                count: 2.0,
                sum: 3.0,
                // Buckets 1 and 2: (1, sqrt 2] and (sqrt 2, 2].
                positive_spans: vec![BucketSpan {
                    offset: 1,
                    length: 2,
                }],
                positive_counts: vec![1.0, 1.0],
                negative_spans: vec![],
                negative_counts: vec![],
                custom_values: None,
                start_timestamp_ms: None,
            },
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let QueryResult::InstantVector(samples) = engine
            .query_instant(&tenant_id("tenant-a"), "histogram_quantile(0.5, h)", 10_000)
            .await
            .expect("a quantile")
        else {
            panic!("expected a vector");
        };
        check!(
            approx_eq(float_value(&samples[0].value), std::f64::consts::SQRT_2),
            "the median sits on the boundary between the two buckets: {}",
            float_value(&samples[0].value)
        );
    }
}

mod native_histogram_planner_path_matches_interpreter {
    use super::*;

    #[derive(bon::Builder)]
    struct ClassicBucketSample {
        upper_bound: &'static str,
        value: f64,
    }

    /// Differential parity for the native-histogram constructs in the planner.
    ///
    /// The recursive planner routes a bare native-histogram selector, native
    /// `histogram_quantile`, and every native accessor: `histogram_count`/`sum`/
    /// `avg`/`stddev`/`stdvar`/`fraction`. Each query MUST claim the operator
    /// `Precomputed` path and MUST match the interpreter byte-for-byte. The test
    /// compares the histogram payloads by value, not by float `==`.
    #[tokio::test]
    pub(crate) async fn native_histogram_planner_path_matches_interpreter() {
        use crate::{DurationExprContext, parse_promql_with_duration_context};

        // Two native-histogram groups (job=api / job=db) so multi-series output and
        // the `__name__` drop are both observable, plus a classic `cls_bucket{le}`
        // float histogram to exercise the classic+native co-routing inside the
        // shared `histogram_quantile` / `histogram_fraction` folds.
        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "t",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "nh"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]),
            300_000,
            two_positive_bucket_histogram(4.0, 6.5),
        );
        store.push_histogram(
            "t",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "nh"),
                crate::test_support::TestLabelPair::new("job", "db"),
            ]),
            300_000,
            two_positive_bucket_histogram(8.0, 20.0),
        );
        for bucket in [
            ClassicBucketSample::builder()
                .upper_bound("1")
                .value(1.0)
                .build(),
            ClassicBucketSample::builder()
                .upper_bound("2")
                .value(3.0)
                .build(),
            ClassicBucketSample::builder()
                .upper_bound("+Inf")
                .value(4.0)
                .build(),
        ] {
            let lbls = labels(&[
                crate::test_support::TestLabelPair::new("__name__", "cls_bucket"),
                crate::test_support::TestLabelPair::new("le", bucket.upper_bound),
            ]);
            store.push_float("t", lbls, 300_000, bucket.value);
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let queries = [
            // The bare native-histogram selector itself (carries the histogram
            // payload + full labelset, including `__name__`).
            QueryAt {
                query: "nh",
                time_ms: 300_000,
            },
            // Native histogram_quantile at two phis.
            QueryAt {
                query: "histogram_quantile(0.5, nh)",
                time_ms: 300_000,
            },
            QueryAt {
                query: "histogram_quantile(0.9, nh)",
                time_ms: 300_000,
            },
            // Every native accessor.
            QueryAt {
                query: "histogram_count(nh)",
                time_ms: 300_000,
            },
            QueryAt {
                query: "histogram_sum(nh)",
                time_ms: 300_000,
            },
            QueryAt {
                query: "histogram_avg(nh)",
                time_ms: 300_000,
            },
            QueryAt {
                query: "histogram_stddev(nh)",
                time_ms: 300_000,
            },
            QueryAt {
                query: "histogram_stdvar(nh)",
                time_ms: 300_000,
            },
            // histogram_fraction carries two scalar bounds.
            QueryAt {
                query: "histogram_fraction(1, 2, nh)",
                time_ms: 300_000,
            },
            QueryAt {
                query: "histogram_fraction(-Inf, +Inf, nh)",
                time_ms: 300_000,
            },
            // The shared folds also work over the classic float buckets.
            QueryAt {
                query: "histogram_quantile(0.5, cls_bucket)",
                time_ms: 300_000,
            },
            QueryAt {
                query: "histogram_fraction(1, 2, cls_bucket)",
                time_ms: 300_000,
            },
        ];

        assert_call_planner_matches_interpreter(&engine, &queries).await;

        // The bare-selector case must surface the native histogram payload (proving
        // the histogram-aware selection actually carried it, not a dropped/empty
        // vector).
        let bare = parse_promql_with_duration_context("nh", DurationExprContext::instant(300_000))
            .expect("parse nh");
        let plan = engine
            .plan_instant_expr("t", &bare, 300_000)
            .await
            .expect("plan nh")
            .expect("nh routes through planner");
        let QueryResult::InstantVector(samples) = engine
            .assemble_planned_instant(plan, 300_000)
            .await
            .expect("assemble nh")
        else {
            panic!("expected vector for nh");
        };
        assert2::assert!(samples.len() == 2);
        assert2::assert!(
            samples
                .iter()
                .all(|sample| matches!(sample.value, SampleValue::Histogram(_)))
        );

        // `histogram_quantiles` (experimental) now routes through the shared
        // `apply_histogram_quantiles` fold and must match the interpreter for both
        // native-histogram and classic bucket inputs, across multiple phis.
        #[cfg(feature = "experimental-functions")]
        for query in [
            "histogram_quantiles(nh, \"q\", 0.5, 0.9)",
            "histogram_quantiles(cls_bucket, \"q\", 0.5)",
        ] {
            let parity = planned_and_interpreted(&engine, query, 300_000).await;
            let via_operators = parity.via_operators;
            let via_interpreter = parity.via_interpreter;
            let normalize = |result: QueryResult| fingerprint_sorted(result, query);
            assert2::assert!(instant_samples_match(
                &normalize(via_interpreter),
                &normalize(via_operators)
            ));
        }
    }
}

mod native_histogram_scalar_arithmetic_drops_invalid_operator_orders {
    use super::*;

    #[tokio::test]
    pub(crate) async fn native_histogram_scalar_arithmetic_drops_invalid_operator_orders() {
        let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
        for query in [
            "histogram_count(2 / request_duration_seconds)",
            "histogram_count(request_duration_seconds + 2)",
        ] {
            let samples = instant_vector(&engine, query, 10_000).await;
            assert2::assert!(samples.is_empty());
        }
    }
}

mod native_histogram_scalar_arithmetic_scales_histograms {
    use super::*;

    struct QueryExpectation {
        query: &'static str,
        value: f64,
    }

    #[tokio::test]
    pub(crate) async fn native_histogram_scalar_arithmetic_scales_histograms() {
        let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
        for expectation in [
            QueryExpectation {
                query: "histogram_count(request_duration_seconds * 2)",
                value: 8.0,
            },
            QueryExpectation {
                query: "histogram_sum(2 * request_duration_seconds)",
                value: 20.0,
            },
            QueryExpectation {
                query: "histogram_count(request_duration_seconds / 2)",
                value: 2.0,
            },
            QueryExpectation {
                query: "histogram_sum(request_duration_seconds / 2)",
                value: 5.0,
            },
        ] {
            let samples = instant_vector(&engine, expectation.query, 10_000).await;
            assert_one_unnamed_float(
                &samples,
                ExpectedLabel {
                    name: "job",
                    label_value: "api",
                },
                expectation.value,
            );
        }
    }
}

mod instant_absent_over_time_treats_native_histograms_as_present {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_absent_over_time_treats_native_histograms_as_present() {
        let store = store_with_request_duration_histogram_at(native_histogram(4.0, 10.0), 90_000);

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                &tenant_id("tenant-a"),
                r#"absent_over_time(request_duration_seconds{job="api"}[1m])"#,
                120_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert2::assert!(samples.is_empty());
    }
}

mod instant_count_and_group_aggregations_include_histograms {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_count_and_group_aggregations_include_histograms() {
        let store = mixed_api_group_store();
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for case in [
            QueryValueCase {
                query: "count by (job) (mixed_metric)",
                value: 2.0,
            },
            QueryValueCase {
                query: "group by (job) (mixed_metric)",
                value: 1.0,
            },
        ] {
            let samples = instant_vector(&engine, case.query, 10_000).await;
            assert_one_unnamed_float(
                &samples,
                ExpectedLabel {
                    name: "job",
                    label_value: "api",
                },
                case.value,
            );
        }
    }
}

mod instant_count_and_present_over_time_include_native_histograms {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_count_and_present_over_time_include_native_histograms() {
        for case in [
            QueryValueCase {
                query: "count_over_time(request_duration_seconds[2m])",
                value: 2.0,
            },
            QueryValueCase {
                query: "present_over_time(request_duration_seconds[2m])",
                value: 1.0,
            },
        ] {
            assert_histogram_series_reduction(case.query, case.value).await;
        }
    }
}

mod instant_first_and_last_over_time_return_native_histograms {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_first_and_last_over_time_return_native_histograms() {
        for case in [
            QueryValueCase {
                query: "histogram_sum(first_over_time(request_duration_seconds[2m]))",
                value: 10.0,
            },
            QueryValueCase {
                query: "histogram_sum(last_over_time(request_duration_seconds[2m]))",
                value: 20.0,
            },
        ] {
            assert_histogram_series_reduction(case.query, case.value).await;
        }
    }
}

mod instant_histogram_avg_returns_native_histogram_average {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_histogram_avg_returns_native_histogram_average() {
        let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
        let samples =
            instant_vector(&engine, "histogram_avg(request_duration_seconds)", 10_000).await;
        check!(samples.len() == 1);
        check!(samples[0].labels.get("job") == Some("api"));
        check!(approx_eq(float_value(&samples[0].value), 2.5));
    }
}

mod instant_histogram_count_returns_native_histogram_count {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_histogram_count_returns_native_histogram_count() {
        let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
        let samples =
            instant_vector(&engine, "histogram_count(request_duration_seconds)", 10_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            4.0,
        );
    }
}

mod instant_histogram_fraction_estimates_native_histogram_bucket_overlap {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_histogram_fraction_estimates_native_histogram_bucket_overlap() {
        let store = two_bucket_histogram_store(6.5);

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(
            &engine,
            "histogram_fraction(1, 2, request_duration_seconds)",
            10_000,
        )
        .await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            0.75,
        );
    }
}

mod instant_histogram_quantile_interpolates_classic_buckets {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_histogram_quantile_interpolates_classic_buckets() {
        let engine = PromqlEngine::new(Arc::new(classic_bucket_store()), EngineOpts::default());
        let samples = instant_vector(
            &engine,
            "histogram_quantile(0.5, http_request_duration_seconds_bucket)",
            10_000,
        )
        .await;
        check!(samples.len() == 1);
        check!(samples[0].labels.get("__name__").is_none());
        check!(samples[0].labels.get("le").is_none());
        check!(samples[0].labels.get("job") == Some("api"));
        check!(approx_eq(float_value(&samples[0].value), 0.25));
    }
}

mod instant_histogram_quantile_interpolates_native_histogram_buckets {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_histogram_quantile_interpolates_native_histogram_buckets() {
        let store = two_bucket_histogram_store(6.5);

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(
            &engine,
            "histogram_quantile(0.5, request_duration_seconds)",
            10_000,
        )
        .await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            2_f64.powf(1.0 / 3.0),
        );
    }
}

mod instant_histogram_stddev_returns_native_histogram_population_stddev {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_histogram_stddev_returns_native_histogram_population_stddev() {
        let store = two_bucket_histogram_store(5.25);

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(
            &engine,
            "histogram_stddev(request_duration_seconds)",
            10_000,
        )
        .await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            0.099_384_473_924_297_3_f64.sqrt(),
        );
    }
}

mod instant_histogram_stdvar_estimates_native_histogram_population_variance {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_histogram_stdvar_estimates_native_histogram_population_variance() {
        let store = two_bucket_histogram_store(5.25);

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(
            &engine,
            "histogram_stdvar(request_duration_seconds)",
            10_000,
        )
        .await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            0.099_384_473_924_297_3,
        );
    }
}

mod instant_histogram_sum_returns_native_histogram_sum {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_histogram_sum_returns_native_histogram_sum() {
        let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
        let samples =
            instant_vector(&engine, "histogram_sum(request_duration_seconds)", 10_000).await;
        check!(samples.len() == 1);
        check!(samples[0].labels.get("job") == Some("api"));
        check!(approx_eq(float_value(&samples[0].value), 10.0));
    }
}

mod instant_min_max_and_std_aggregations_ignore_histograms {
    use super::*;

    struct AggregationExpectation {
        query: &'static str,
        expected: f64,
    }

    #[tokio::test]
    pub(crate) async fn instant_min_max_and_std_aggregations_ignore_histograms() {
        let store = mixed_metric_store(
            "mixed_metric",
            &[
                MixedMetricFloatSample::builder()
                    .instance("a")
                    .value(4.0)
                    .build(),
                MixedMetricFloatSample::builder()
                    .instance("b")
                    .value(8.0)
                    .build(),
            ],
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for expectation in [
            AggregationExpectation {
                query: "min by (job) (mixed_metric)",
                expected: 4.0,
            },
            AggregationExpectation {
                query: "max by (job) (mixed_metric)",
                expected: 8.0,
            },
            AggregationExpectation {
                query: "stddev by (job) (mixed_metric)",
                expected: 2.0,
            },
            AggregationExpectation {
                query: "stdvar by (job) (mixed_metric)",
                expected: 4.0,
            },
        ] {
            let samples = instant_vector(&engine, expectation.query, 10_000).await;
            assert_one_unnamed_float(
                &samples,
                ExpectedLabel {
                    name: "job",
                    label_value: "api",
                },
                expectation.expected,
            );
        }
    }
}

mod instant_sum_aggregation_combines_native_histograms_with_different_span_layouts {
    use super::{native_histogram_store::single_bucket_instance_histogram, *};

    #[tokio::test]
    pub(crate) async fn instant_sum_aggregation_combines_native_histograms_with_different_span_layouts()
     {
        let left = single_bucket_instance_histogram(4.0, 10.0, 0, 0, 1.0);
        let right = single_bucket_instance_histogram(6.0, 20.0, 0, 1, 2.0);

        let store = instance_histogram_store(InstanceHistograms { a: left, b: right });

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples =
            instant_vector(&engine, "sum by (job) (request_duration_seconds)", 10_000).await;
        assert2::assert!(samples.len() == 1);
        let SampleValue::Histogram(histogram) = &samples[0].value else {
            panic!("expected histogram");
        };
        assert2::assert!(approx_eq(histogram.count, 10.0));
        assert2::assert!(approx_eq(histogram.sum, 30.0));
        assert2::assert!(
            &histogram.positive_spans
                == &vec![BucketSpan {
                    offset: 0,
                    length: 2,
                }]
        );
        assert2::assert!(&histogram.positive_counts == &vec![1.0, 2.0]);
    }
}

mod instant_sum_aggregation_downscales_native_histograms {
    use super::{native_histogram_store::single_bucket_instance_histogram, *};

    #[tokio::test]
    pub(crate) async fn instant_sum_aggregation_downscales_native_histograms() {
        let left = single_bucket_instance_histogram(4.0, 10.0, 0, 0, 1.0);
        let right = single_bucket_instance_histogram(6.0, 20.0, 1, 0, 2.0);

        let store = instance_histogram_store(InstanceHistograms { a: left, b: right });

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                &tenant_id("tenant-a"),
                "sum by (job) (request_duration_seconds)",
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected instant vector");
        };
        assert2::assert!(samples.len() == 1);
        let SampleValue::Histogram(histogram) = &samples[0].value else {
            panic!("expected histogram");
        };
        assert2::assert!(histogram.schema == 0);
        assert2::assert!((histogram.count - 10.0).abs() < f64::EPSILON);
        assert2::assert!((histogram.sum - 30.0).abs() < f64::EPSILON);
        assert2::assert!(&histogram.positive_counts == &vec![3.0]);
    }
}

mod instant_sum_and_avg_aggregations_combine_compatible_native_histograms {
    use super::*;

    #[derive(bon::Builder)]
    struct HistogramAggregateExpectation {
        query: &'static str,
        count: f64,
        sum: f64,
        average: f64,
    }

    #[tokio::test]
    pub(crate) async fn instant_sum_and_avg_aggregations_combine_compatible_native_histograms() {
        let mut left = native_histogram(4.0, 10.0);
        left.zero_count = 1.0;
        left.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 2,
        }];
        left.positive_counts = vec![1.0, 2.0];
        let mut right = native_histogram(6.0, 20.0);
        right.zero_count = 2.0;
        right.positive_spans = left.positive_spans.clone();
        right.positive_counts = vec![2.0, 2.0];

        let store = instance_histogram_store(InstanceHistograms { a: left, b: right });

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for expected in [
            HistogramAggregateExpectation::builder()
                .query("sum by (job) (request_duration_seconds)")
                .count(10.0)
                .sum(30.0)
                .average(3.0)
                .build(),
            HistogramAggregateExpectation::builder()
                .query("avg by (job) (request_duration_seconds)")
                .count(5.0)
                .sum(15.0)
                .average(3.0)
                .build(),
        ] {
            let count = engine
                .query_instant(
                    &tenant_id("tenant-a"),
                    &format!("histogram_count({})", expected.query),
                    10_000,
                )
                .await
                .unwrap();
            let sum = engine
                .query_instant(
                    &tenant_id("tenant-a"),
                    &format!("histogram_sum({})", expected.query),
                    10_000,
                )
                .await
                .unwrap();
            let avg = engine
                .query_instant(
                    &tenant_id("tenant-a"),
                    &format!("histogram_avg({})", expected.query),
                    10_000,
                )
                .await
                .unwrap();

            assert_single_float_sample(&count, "api", expected.count, expected.query);
            assert_single_float_sample(&sum, "api", expected.sum, expected.query);
            assert_single_float_sample(&avg, "api", expected.average, expected.query);
        }
    }
}

mod instant_sum_and_avg_aggregations_omit_mixed_float_and_histogram_groups {
    use super::*;

    #[tokio::test]
    pub(crate) async fn instant_sum_and_avg_aggregations_omit_mixed_float_and_histogram_groups() {
        let mut store = mixed_api_group_store();
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "mixed_metric"),
                crate::test_support::TestLabelPair::new("job", "web"),
                crate::test_support::TestLabelPair::new("instance", "float"),
            ]),
            10_000,
            6.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for query in ["sum by (job) (mixed_metric)", "avg by (job) (mixed_metric)"] {
            let samples = instant_vector(&engine, query, 10_000).await;
            assert2::assert!(samples.len() == 1);
            assert2::assert!(samples[0].labels.get("job") == Some("web"));
            assert2::assert!(approx_eq(float_value(&samples[0].value), 6.0));
        }
    }
}

mod instant_topk_and_bottomk_ignore_histograms {
    use super::*;

    struct RankingCase {
        query: &'static str,
        expected_instance: &'static str,
        expected_value: f64,
    }

    #[tokio::test]
    pub(crate) async fn instant_topk_and_bottomk_ignore_histograms() {
        let store = mixed_metric_store(
            "memory_bytes",
            &[
                MixedMetricFloatSample::builder()
                    .instance("a")
                    .value(1.0)
                    .build(),
                MixedMetricFloatSample::builder()
                    .instance("b")
                    .value(3.0)
                    .build(),
            ],
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for case in [
            RankingCase {
                query: "topk(1, memory_bytes)",
                expected_instance: "b",
                expected_value: 3.0,
            },
            RankingCase {
                query: "bottomk(1, memory_bytes)",
                expected_instance: "a",
                expected_value: 1.0,
            },
        ] {
            let samples = instant_vector(&engine, case.query, 10_000).await;
            assert2::assert!(samples.len() == 1);
            assert2::assert!(samples[0].labels.get("__name__") == Some("memory_bytes"));
            assert2::assert!(samples[0].labels.get("instance") == Some(case.expected_instance));
            assert2::assert!(approx_eq(
                float_value(&samples[0].value),
                case.expected_value
            ));
        }
    }
}
