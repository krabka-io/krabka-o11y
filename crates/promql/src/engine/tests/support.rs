//! Shared test helpers for this test area.
use super::*;

pub(crate) mod assert_aggregate_nan_staleness {
    use super::*;

    /// Asserts the staleness semantics for the `nan_metric` parity queries.
    ///
    /// `sum` keeps the genuine NaN as a NaN value, not as a stale marker. `count`
    /// drops the stale-NaN marker before it counts, so the result is 2, not 3.
    pub(crate) fn assert_aggregate_nan_staleness(
        query: &str,
        via_operators: &[crate::InstantSample],
    ) {
        if query == "sum(nan_metric)" {
            assert2::assert!(via_operators.len() == 1);
            let value = float_value(&via_operators[0].value);
            assert2::assert!(value.is_nan());
            assert2::assert!(!super::super::super::is_stale_nan(value));
        }
        if query == "count(nan_metric)" {
            assert2::assert!(via_operators.len() == 1);
            let value = float_value(&via_operators[0].value);
            assert2::assert!(approx_eq(value, 2.0));
        }
    }
}

pub(crate) mod assert_filled_many_side {
    use super::*;

    /// Checks a `fill_right(0)` group match of `http_requests_total` instances
    /// `a` = 100, `b` = 50 (job `api`) and `c` = 7 (job `worker`) against a `job="api"`
    /// one side of 10 with `region="east"`: the matched instances gain the region
    /// and the 10, and the unmatched `c` keeps its value without a region.
    pub(crate) fn assert_filled_many_side(samples: &[crate::InstantSample]) {
        let mut values = BTreeMap::new();
        for sample in samples {
            values.insert(
                sample.labels.get("instance").expect("instance label"),
                FilledSideValue {
                    region: sample.labels.get("region"),
                    value: float_value(&sample.value),
                },
            );
        }
        assert2::assert!(values.len() == 3);
        assert2::assert!(values["a"].region == Some("east"));
        assert2::assert!(approx_eq(values["a"].value, 110.0));
        assert2::assert!(values["b"].region == Some("east"));
        assert2::assert!(approx_eq(values["b"].value, 60.0));
        assert2::assert!(values["c"].region == None);
        assert2::assert!(approx_eq(values["c"].value, 7.0));
    }

    struct FilledSideValue<'a> {
        region: Option<&'a str>,
        value: f64,
    }
}

pub(crate) mod assert_minmax_nan_ignoring {
    use super::*;

    /// Asserts the NaN-ignore rule for `min` and `max` on the `minmax_nan` queries.
    ///
    /// The test checks absolute values. `min` and `max` take the extremum of the
    /// mixed group over its non-NaN samples: min=1 and max=4. The engine keeps the
    /// all-NaN group with a NaN result and does not drop the series.
    pub(crate) fn assert_minmax_nan_ignoring(query: &str, via_operators: &[crate::InstantSample]) {
        // Look up a group's value by its `g` label.
        let by_group = |g: &str| -> f64 {
            let sample = via_operators
                .iter()
                .find(|sample| sample.labels.get("g") == Some(g))
                .unwrap_or_else(|| panic!("`{query}`: group g={g} missing"));
            float_value(&sample.value)
        };
        match query {
            "min by (g) (minmax_nan)" => {
                assert2::assert!(via_operators.len() == 2);
                let mixed = by_group("mixed");
                assert2::assert!(approx_eq(mixed, 1.0));
                let allnan = by_group("allnan");
                assert2::assert!(allnan.is_nan());
            }
            "max by (g) (minmax_nan)" => {
                assert2::assert!(via_operators.len() == 2);
                let mixed = by_group("mixed");
                assert2::assert!(approx_eq(mixed, 4.0));
                let allnan = by_group("allnan");
                assert2::assert!(allnan.is_nan());
            }
            // `min`/`max` with no grouping fold both groups together: the global
            // extremum is over the only non-NaN samples (the mixed group's
            // {4, 1}), so min=1 and max=4 (the all-NaN group is ignored, but its
            // presence does not force a NaN because the mixed group has finite
            // values).
            "min(minmax_nan)" => {
                assert2::assert!(via_operators.len() == 1);
                let value = float_value(&via_operators[0].value);
                assert2::assert!(approx_eq(value, 1.0));
            }
            "max(minmax_nan)" => {
                assert2::assert!(via_operators.len() == 1);
                let value = float_value(&via_operators[0].value);
                assert2::assert!(approx_eq(value, 4.0));
            }
            _ => {}
        }
    }
}

pub(crate) mod assert_one_unnamed_float {
    use super::*;

    /// One label's name and value.
    #[derive(Clone, Copy)]
    pub(crate) struct ExpectedLabel<'a> {
        pub(crate) name: &'a str,
        pub(crate) label_value: &'a str,
    }

    /// Checks that `samples` is one float sample without a metric name, carrying
    /// the `label`, whose value is approximately `expected`.
    pub(crate) fn assert_one_unnamed_float(
        samples: &[crate::InstantSample],
        label: ExpectedLabel<'_>,
        expected: f64,
    ) {
        assert2::assert!(samples.len() == 1);
        assert2::assert!(samples[0].labels.get("__name__").is_none());
        assert2::assert!(samples[0].labels.get(label.name) == Some(label.label_value));
        assert2::assert!(approx_eq(float_value(&samples[0].value), expected));
    }
}

pub(crate) mod assert_sparse_aggregate_excludes_no_value {
    use super::*;

    /// Asserts the sparse aggregate-over-rate rule for the `sparse_total` queries.
    ///
    /// The test checks absolute values. The engine excludes the sparse no-value
    /// series from its group. The `g="mix"` group survives with only the
    /// contribution of its dense member. The result has no row for the
    /// all-no-value `g="allsparse"` group: the series is absent, not present with
    /// NaN.
    pub(crate) fn assert_sparse_aggregate_excludes_no_value(
        query: &str,
        via_operators: &[crate::InstantSample],
    ) {
        let group_value = |g: &str| -> Option<f64> {
            via_operators
                .iter()
                .find(|sample| sample.labels.get("g") == Some(g))
                .map(|sample| float_value(&sample.value))
        };
        match query {
            // g="mix" survives (its dense member has a rate); g="allsparse" has
            // no value-bearing member, so it is absent. Only one row total.
            "sum by (g) (rate(sparse_total[2m]))"
            | "avg by (g) (rate(sparse_total[2m]))"
            | "min by (g) (rate(sparse_total[2m]))"
            | "max by (g) (rate(sparse_total[2m]))"
            | "count by (g) (rate(sparse_total[2m]))"
            | "group by (g) (rate(sparse_total[2m]))" => {
                assert2::assert!(via_operators.len() == 1);
                assert2::assert!(group_value("mix").is_some());
                assert2::assert!(group_value("allsparse").is_none());
                // `count`/`group` over g=mix see exactly the one dense member.
                if query == "count by (g) (rate(sparse_total[2m]))" {
                    assert2::assert!(approx_eq(group_value("mix").unwrap(), 1.0));
                }
                if query == "group by (g) (rate(sparse_total[2m]))" {
                    assert2::assert!(approx_eq(group_value("mix").unwrap(), 1.0));
                }
            }
            // No grouping: the global aggregate is over the single dense rate.
            "count (rate(sparse_total[2m]))" => {
                assert2::assert!(via_operators.len() == 1);
                assert2::assert!(approx_eq(float_value(&via_operators[0].value), 1.0));
            }
            "sum (rate(sparse_total[2m]))" => {
                assert2::assert!(via_operators.len() == 1);
            }
            // The `*_over_time` window strands every sparse member, leaving only
            // the dense member in g=mix; g=allsparse is absent.
            "count by (g) (avg_over_time(sparse_total[30s]))" => {
                assert2::assert!(via_operators.len() == 1);
                assert2::assert!(approx_eq(group_value("mix").unwrap(), 1.0));
                assert2::assert!(group_value("allsparse").is_none());
            }
            _ => {}
        }
    }
}

pub(crate) mod classic_bucket_store {
    use super::*;

    #[derive(bon::Builder)]
    struct ClassicBucket {
        upper_bound: &'static str,
        cumulative_count: f64,
    }

    /// A store holding classic `http_request_duration_seconds_bucket{job="api"}`
    /// buckets at 10s: `le` 0.1 = 0, 0.2 = 1, 0.4 = 3 and `+Inf` = 3.
    pub(crate) fn classic_bucket_store() -> InMemoryMetricStore {
        let mut store = InMemoryMetricStore::new();
        for bucket in [
            ClassicBucket::builder()
                .upper_bound("0.1")
                .cumulative_count(0.0)
                .build(),
            ClassicBucket::builder()
                .upper_bound("0.2")
                .cumulative_count(1.0)
                .build(),
            ClassicBucket::builder()
                .upper_bound("0.4")
                .cumulative_count(3.0)
                .build(),
            ClassicBucket::builder()
                .upper_bound("+Inf")
                .cumulative_count(3.0)
                .build(),
        ] {
            store.push_float(
                "tenant-a",
                labels(&[
                    crate::test_support::TestLabelPair::new(
                        "__name__",
                        "http_request_duration_seconds_bucket",
                    ),
                    crate::test_support::TestLabelPair::new("job", "api"),
                    crate::test_support::TestLabelPair::new("le", bucket.upper_bound),
                ]),
                10_000,
                bucket.cumulative_count,
            );
        }
        store
    }
}

pub(crate) mod instant_experimental_functions_require_feature {
    #[cfg(not(feature = "experimental-functions"))]
    use super::*;

    #[cfg(not(feature = "experimental-functions"))]
    #[yare::parameterized(
    limitk = { "limitk(2, memory_bytes)", 10_000 },
    limit_ratio = { "limit_ratio(0.5, memory_bytes)", 10_000 },
    double_exponential_smoothing = {
        "double_exponential_smoothing(gauge[5m], 0.5, 0.5)",
        120_000
    },
)]
    pub(crate) fn instant_functions_require_experimental_feature(query: &str, time_ms: i64) {
        let store = InMemoryMetricStore::new();
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the async test runtime builds");
        let error = runtime.block_on(async {
            engine
                .query_instant(&tenant_id("tenant-a"), query, time_ms)
                .await
                .unwrap_err()
        });

        assert2::assert!(matches!(error, PromqlError::Unsupported(_)));
        assert2::assert!(format!("{error}").contains("experimental-functions"));
    }
}

pub(crate) mod native_histogram {
    use super::*;

    pub(crate) fn native_histogram(count: f64, sum: f64) -> NativeHistogram {
        NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::No,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count,
            sum,
            positive_spans: vec![],
            positive_counts: vec![],
            negative_spans: vec![],
            negative_counts: vec![],
            custom_values: None,
            start_timestamp_ms: None,
        }
    }
}

pub(crate) mod query_results_match {
    use super::*;

    /// Compares two whole [`QueryResult`]s for the parity tests below.
    ///
    /// The comparison is NaN-aware across the scalar, vector, matrix, and string
    /// shapes, so a genuine NaN equals a genuine NaN. The caller pre-sorts vectors
    /// by fingerprint.
    pub(crate) fn query_results_match(left: &QueryResult, right: &QueryResult) -> bool {
        match left {
            QueryResult::Scalar { ts_ms, value } => match right {
                QueryResult::Scalar {
                    ts_ms: other_ts_ms,
                    value: other_value,
                } => ts_ms == other_ts_ms && value.to_bits() == other_value.to_bits(),
                _ => false,
            },
            QueryResult::InstantVector(samples) => match right {
                QueryResult::InstantVector(other_samples) => {
                    instant_samples_match(samples, other_samples)
                }
                _ => false,
            },
            QueryResult::RangeMatrix(_) => {
                matches!(right, QueryResult::RangeMatrix(_)) && range_matrices_match(left, right)
            }
            QueryResult::Str { ts_ms, value } => match right {
                QueryResult::Str {
                    ts_ms: other_ts_ms,
                    value: other_value,
                } => ts_ms == other_ts_ms && value == other_value,
                _ => false,
            },
        }
    }
}

pub(crate) mod queue_depth_engine {
    use super::*;

    /// An engine over `queue_depth{job="api"}`: 1, 2 and 3 at 0s, 60s and 120s.
    pub(crate) fn queue_depth_engine() -> PromqlEngine<InMemoryMetricStore> {
        let store = queue_depth_store(&[(0, 1.0), (60_000, 2.0), (120_000, 3.0)]);
        PromqlEngine::new(Arc::new(store), EngineOpts::default())
    }
}

pub(crate) mod range_matrices_match {
    use super::*;

    /// Compares two `RangeMatrix` results for the range parity test.
    ///
    /// The comparison is bit-exact on float sample values, so a genuine NaN equals
    /// a genuine NaN. A plain `PartialEq` fails on `NaN == NaN`. Series order,
    /// labelsets, per-step timestamps, and gaps must all match.
    pub(crate) fn range_matrices_match(left: &QueryResult, right: &QueryResult) -> bool {
        let QueryResult::RangeMatrix(left) = left else {
            return false;
        };
        let QueryResult::RangeMatrix(right) = right else {
            return false;
        };
        if left.len() != right.len() {
            return false;
        }
        let mut series_index = 0;
        while series_index < left.len() {
            let left_series = &left[series_index];
            let right_series = &right[series_index];
            if left_series.labels != right_series.labels
                || left_series.samples.len() != right_series.samples.len()
            {
                return false;
            }
            let mut sample_index = 0;
            while sample_index < left_series.samples.len() {
                let left_sample = &left_series.samples[sample_index];
                let right_sample = &right_series.samples[sample_index];
                if left_sample.timestamp_ms != right_sample.timestamp_ms {
                    return false;
                }
                let values_match = if let SampleValue::Float(left_value) = &left_sample.value {
                    if let SampleValue::Float(right_value) = &right_sample.value {
                        left_value.to_bits() == right_value.to_bits()
                    } else {
                        false
                    }
                } else {
                    left_sample.value == right_sample.value
                };
                if !values_match {
                    return false;
                }
                sample_index += 1;
            }
            series_index += 1;
        }
        true
    }
}

pub(crate) mod range_query_routes_through_planner {
    use promql_parser::parser::Expr;

    use crate::{DurationExprContext, parse_promql_with_duration_context};

    /// Whether the range-query gate routes `query`, parsed for the `grid`
    /// context with its outer parentheses stripped, through the planner.
    pub(crate) fn range_query_routes_through_planner(
        query: &str,
        grid: DurationExprContext,
    ) -> bool {
        let expr = parse_promql_with_duration_context(query, grid)
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let mut probe = &expr;
        while let Expr::Paren(paren) = probe {
            probe = &paren.expr;
        }
        super::super::super::range_expr_routes_through_planner(probe)
    }
}

pub(crate) mod region_info_engine {
    use super::*;

    /// One `http_requests_total{job, instance}` series and its value at 10s.
    #[derive(Clone, Copy)]
    pub(crate) struct InstanceRequests<'a> {
        pub(crate) job: &'a str,
        pub(crate) instance: &'a str,
        pub(crate) total: f64,
    }

    /// An engine over each of `requests` and
    /// `target_info{job="api",region="east"}` = 10, all at 10s.
    pub(crate) fn region_info_engine(
        requests: &[InstanceRequests<'_>],
    ) -> PromqlEngine<InMemoryMetricStore> {
        let mut store = InMemoryMetricStore::new();
        for series in requests {
            store.push_float(
                "tenant-a",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
                    crate::test_support::TestLabelPair::new("job", series.job),
                    crate::test_support::TestLabelPair::new("instance", series.instance),
                ]),
                10_000,
                series.total,
            );
        }
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "target_info"),
                crate::test_support::TestLabelPair::new("job", "api"),
                crate::test_support::TestLabelPair::new("region", "east"),
            ]),
            10_000,
            10.0,
        );
        PromqlEngine::new(Arc::new(store), EngineOpts::default())
    }
}

pub(crate) mod set_op_store {
    use super::*;

    struct SetOpSample {
        instance: &'static str,
        value: f64,
    }

    pub(crate) fn set_op_store() -> InMemoryMetricStore {
        let mut store = InMemoryMetricStore::new();
        for sample in [
            SetOpSample {
                instance: "a",
                value: 1.0,
            },
            SetOpSample {
                instance: "b",
                value: 2.0,
            },
        ] {
            store.push_float(
                "tenant-a",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "up"),
                    crate::test_support::TestLabelPair::new("instance", sample.instance),
                    crate::test_support::TestLabelPair::new("job", "api"),
                ]),
                10_000,
                sample.value,
            );
        }
        for sample in [
            SetOpSample {
                instance: "b",
                value: 20.0,
            },
            SetOpSample {
                instance: "c",
                value: 30.0,
            },
        ] {
            store.push_float(
                "tenant-a",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "target_info"),
                    crate::test_support::TestLabelPair::new("instance", sample.instance),
                    crate::test_support::TestLabelPair::new("region", "east"),
                ]),
                10_000,
                sample.value,
            );
        }
        store
    }
}

pub(crate) mod short_lookback_up_engine {
    use super::*;

    /// An engine with a 30s lookback over `up{job="api"}`: 1 at 60s and 2 at 120s.
    pub(crate) fn short_lookback_up_engine() -> PromqlEngine<InMemoryMetricStore> {
        lookback_engine(
            SeriesFixture::new(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]))
            .at(60_000, 1.0)
            .at(120_000, 2.0)
            .store(),
            millis(30_000),
        )
    }
}

pub(crate) mod sort_instant_result {
    use super::*;

    /// Sorts an instant-vector result by fingerprint in place.
    ///
    /// The function does nothing for the other result shapes. `query_results_match`
    /// can then compare vectors independent of order.
    pub(crate) fn sort_instant_result(result: QueryResult) -> QueryResult {
        match result {
            QueryResult::InstantVector(mut samples) => {
                samples.sort_by_key(|sample| sample.labels.fingerprint());
                QueryResult::InstantVector(samples)
            }
            other => other,
        }
    }
}
