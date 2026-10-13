use krabka_metrics::LimitError;

use super::{
    super::{
        aggregation::{AggregateOp, AggregateState},
        range_functions::{over_time_mean, over_time_sum},
    },
    *,
};
use crate::{Annotations, InstantSample, MergedMetricStore, PromqlLabels, RangeSeries, WalHead};

struct ExpectedVectorSample {
    labels: PromqlLabels,
    value: f64,
}

struct AggregateValue {
    operation: AggregateOp,
    value: f64,
}

#[derive(Debug, PartialEq)]
struct RawFloatRowBits {
    fingerprint: u64,
    timestamp_ms: i64,
    value_bits: u64,
    start_timestamp_ms: Option<i64>,
}

struct StoredSampleCase {
    instance: &'static str,
    job: &'static str,
    timestamp_ms: i64,
    value: f64,
}

struct QueryExpectation {
    query: &'static str,
    timestamp_ms: i64,
    expected: QueryResult,
}

struct QueryValueCase {
    query: &'static str,
    timestamp_ms: i64,
    value: f64,
}

struct QueryScalarCase {
    query: &'static str,
    value: f64,
}

struct ByteIdentityCase {
    identity: Option<Vec<u8>>,
    value: f64,
}

struct OptionalMetricNameCase {
    name: Option<&'static str>,
    value: f64,
}

struct HistogramValueCase {
    instance: &'static str,
    count: f64,
    sum: f64,
}

struct FloatAggregateInput {
    values: Vec<f64>,
    sum: f64,
    average: f64,
}

struct HistogramQueryCase {
    query: &'static str,
    expected: NativeHistogram,
}

struct InstanceSampleCase {
    instance: &'static str,
    timestamp_ms: i64,
    value: f64,
}

struct LimitBudgetCase {
    max_samples: usize,
    max_fetched_series: usize,
    error: Option<LimitError>,
}

struct TimestampRangeErrorCase {
    timestamp_ms: i64,
    range: &'static str,
    message: &'static str,
}

struct TimestampValueCase {
    timestamp_ms: i64,
    value: f64,
}

struct OptionalFloatCase {
    max_samples: usize,
    expected: Option<f64>,
}

struct AcceptedBudgetCase {
    max_samples: usize,
    accepted: bool,
}

struct QueryStatsCase {
    query: &'static str,
    value: f64,
    total: u64,
    peak: usize,
}

impl AggregateValue {
    fn new(operation: AggregateOp, value: f64) -> Self {
        Self { operation, value }
    }
}

impl ExpectedVectorSample {
    fn new(labels: PromqlLabels, value: f64) -> Self {
        Self { labels, value }
    }
}

fn vector(time_ms: i64, rows: &[ExpectedVectorSample]) -> QueryResult {
    QueryResult::InstantVector(
        rows.iter()
            .map(|row| InstantSample {
                labels: row.labels.clone(),
                ts_ms: time_ms,
                value: SampleValue::Float(row.value),
                drop_name: false,
            })
            .collect(),
    )
}

/// The order a test store receives its samples in.
#[derive(Clone, Copy, PartialEq, Eq)]
enum InsertionOrder {
    Forward,
    Reversed,
}

/// A store holding `m{instance="<index>"}` at 10s for each of `sample_values`
/// by position, inserted in `order`, for tenant `t`.
fn instance_store(sample_values: &[f64], order: InsertionOrder) -> InMemoryMetricStore {
    let mut indexed = sample_values
        .iter()
        .copied()
        .enumerate()
        .collect::<Vec<_>>();
    if order == InsertionOrder::Reversed {
        indexed.reverse();
    }
    let mut store = InMemoryMetricStore::new();
    for indexed_sample in indexed {
        let index = indexed_sample.0;
        let sample_value = indexed_sample.1;
        store.push_float(
            "t",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "m"),
                crate::test_support::TestLabelPair::new("instance", &index.to_string()),
            ]),
            10_000,
            sample_value,
        );
    }
    store
}

/// Checks that the public instant `query` for tenant `t` at 10s returns the
/// interpreter's single float sample bit-for-bit, and nothing else, without
/// annotations. A value mismatch names the query and the `inserted` values,
/// in insertion order.
async fn assert_scalar_aggregate_matches_interpreter<S: crate::MetricStore>(
    engine: &PromqlEngine<S>,
    query: &str,
    inserted: &[f64],
) {
    let expr = promql_parser::parser::parse(query).unwrap();
    let annotated = engine
        .query_instant_with_annotations(&tenant_id("t"), query, 10_000)
        .await
        .unwrap();
    let expected = engine.eval_instant_expr("t", &expr, 10_000).await.unwrap();
    let annotations = annotated.annotations;
    let QueryResult::InstantVector(mut actual) = annotated.result else {
        panic!("expected vector")
    };
    let QueryResult::InstantVector(mut expected) = expected else {
        panic!("expected vector")
    };
    assert2::assert!(actual.len() == 1 && expected.len() == 1);
    let SampleValue::Float(actual_value) = &actual[0].value else {
        panic!("expected float")
    };
    let SampleValue::Float(expected_value) = &expected[0].value else {
        panic!("expected float")
    };
    assert2::assert!(
        actual_value.to_bits() == expected_value.to_bits(),
        "{query}: {inserted:?}"
    );
    actual[0].value = SampleValue::Float(0.0);
    expected[0].value = SampleValue::Float(0.0);
    assert2::assert!(
        AnnotatedQueryResult {
            result: QueryResult::InstantVector(actual),
            annotations
        } == AnnotatedQueryResult {
            result: QueryResult::InstantVector(expected),
            annotations: Annotations::default()
        }
    );
}

#[test]
fn last_over_time_operation_folds_match_finite_aggregate_bits() {
    let tiny = f64::from_bits(1);
    let mut cases = vec![
        vec![0.0],
        vec![-0.0],
        vec![-0.0, -0.0],
        vec![0.0, -0.0],
        vec![tiny, tiny, -tiny],
        vec![f64::MIN_POSITIVE, tiny, -f64::MIN_POSITIVE],
        vec![1e16, 1.0, -1e16],
        vec![1e16, -1e16, 1.0],
        vec![f64::MAX, f64::MAX, -f64::MAX],
        vec![f64::MAX, -f64::MAX, f64::MAX],
        vec![f64::MAX, f64::MAX, -f64::MAX, -f64::MAX],
        vec![f64::MAX, f64::MAX, 1.0, -f64::MAX],
    ];
    for first in [0.0, -0.0, tiny, -tiny, 1.0, -1.0, f64::MAX, -f64::MAX] {
        for second in [0.0, -0.0, tiny, -tiny, 1.0, -1.0, f64::MAX, -f64::MAX] {
            cases.push(vec![first, second]);
        }
    }
    for values in cases {
        let mut state = AggregateState::new(PromqlLabels::new());
        for &value in &values {
            state.push_float(value);
        }
        for aggregate in [
            AggregateValue::new(AggregateOp::Sum, over_time_sum(values.iter().copied())),
            AggregateValue::new(AggregateOp::Avg, over_time_mean(values.iter().copied())),
        ] {
            let Some(SampleValue::Float(expected)) = aggregate.operation.finish(&state) else {
                panic!("expected float")
            };
            assert2::assert!(
                aggregate.value.to_bits() == expected.to_bits(),
                "{values:?}"
            );
        }
    }
    // These inputs distinguish compensation and canonical accumulation order.
    assert2::assert!(over_time_sum([1e16, 1.0, -1e16].into_iter()).to_bits() == 1.0_f64.to_bits());
    assert2::assert!(over_time_sum([f64::MAX, f64::MAX, -f64::MAX].into_iter()).is_infinite());
    assert2::assert!(
        over_time_sum([f64::MAX, -f64::MAX, f64::MAX].into_iter()).to_bits() == f64::MAX.to_bits()
    );
}

#[tokio::test]
async fn last_over_time_aggregate_keeps_nonfinite_sign_and_payload_bits() {
    let positive_nan = f64::from_bits(0x7ff8_0000_0000_0042);
    let negative_nan = f64::from_bits(0xfff8_0000_0000_0042);
    let other_nan = f64::from_bits(0x7ff8_0000_0000_0011);
    for values in [
        vec![positive_nan],
        vec![negative_nan],
        vec![positive_nan, negative_nan, other_nan],
        vec![negative_nan, positive_nan, 0.0],
        vec![0.0, positive_nan, negative_nan],
        vec![1.0, other_nan, positive_nan],
        vec![f64::INFINITY],
        vec![f64::NEG_INFINITY],
        vec![f64::INFINITY, f64::INFINITY, 1.0],
        vec![f64::INFINITY, f64::NEG_INFINITY, 0.0],
        vec![f64::NEG_INFINITY, f64::INFINITY, -0.0],
    ] {
        let store = instance_store(&values, InsertionOrder::Forward);
        let engine = PromqlEngine::new(
            Arc::new(MergedMetricStore::new(InMemoryMetricStore::new(), store)),
            EngineOpts::default(),
        );
        for query in ["sum(last_over_time(m[30m]))", "avg(last_over_time(m[30m]))"] {
            assert_scalar_aggregate_matches_interpreter(&engine, query, &values).await;
        }
    }
}

#[tokio::test]
async fn last_over_time_aggregate_keeps_finite_interpreter_bits_in_both_insertion_orders() {
    for values in [
        vec![f64::MAX, f64::MAX, -f64::MAX],
        vec![f64::MAX, -f64::MAX, f64::MAX],
        vec![f64::MAX, f64::MAX, -f64::MAX, -f64::MAX],
        vec![1e16, 1.0, -1e16],
        vec![1e16, -1e16, 1.0],
    ] {
        for order in [InsertionOrder::Forward, InsertionOrder::Reversed] {
            let mut inserted = values.clone();
            if order == InsertionOrder::Reversed {
                inserted.reverse();
            }
            let store = instance_store(&values, order);
            let engine = PromqlEngine::new(
                Arc::new(MergedMetricStore::new(
                    InMemoryMetricStore::new(),
                    WalHead::from_store(store),
                )),
                EngineOpts::default(),
            );
            for query in ["sum(last_over_time(m[30m]))", "avg(last_over_time(m[30m]))"] {
                assert_scalar_aggregate_matches_interpreter(&engine, query, &inserted).await;
            }
        }
    }
}

#[tokio::test]
async fn last_over_time_aggregate_keeps_empty_vectors_with_nonfinite_orphans() {
    for value in [0.0, f64::INFINITY, f64::from_bits(0xfff8_0000_0000_0042)] {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "t",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "m")]),
            1_000,
            value,
        );
        let mut orphan = store.floats["t"].iter().next().unwrap().clone();
        orphan.fp = 0;
        let canonical_labels = Arc::clone(&orphan.labels);
        let canonical_fp = canonical_labels.fingerprint();
        let stored_rows = store.floats.get_mut("t").unwrap();
        stored_rows.retain(|_| false);
        stored_rows.push(orphan);
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let matchers = vec![crate::PromqlMatcher::new(
            "__name__",
            krabka_blockstore::MatchOp::Eq,
            "m",
        )];
        let result = engine
            .last_float_rows("t", &matchers, 1, 3_000)
            .await
            .unwrap();
        let total = result.total;
        let rows = result.rows;
        let raw_rows = rows
            .iter()
            .map(|row| RawFloatRowBits {
                fingerprint: row.fp,
                timestamp_ms: row.ts_ms,
                value_bits: row.value.to_bits(),
                start_timestamp_ms: row.start_timestamp_ms,
            })
            .collect::<Vec<_>>();
        assert2::assert!(total == 1);
        assert2::assert!(
            raw_rows
                == vec![RawFloatRowBits {
                    fingerprint: 0,
                    timestamp_ms: 1_000,
                    value_bits: value.to_bits(),
                    start_timestamp_ms: None,
                }]
        );
        let canonical = engine
            .labels_by_fingerprint_sets("t", &[matchers], 0, 3_000)
            .await
            .unwrap();
        assert2::assert!(canonical.as_ref() == &BTreeMap::from([(canonical_fp, canonical_labels)]));
        assert2::assert!(!canonical.contains_key(&0));
        for query in ["sum(last_over_time(m[3s]))", "avg(last_over_time(m[3s]))"] {
            let actual = engine
                .query_instant_with_annotations(&tenant_id("t"), query, 3_000)
                .await
                .unwrap();
            assert2::assert!(
                actual
                    == AnnotatedQueryResult {
                        result: vector(3_000, &[]),
                        annotations: Annotations::default()
                    }
            );
        }
    }
}

#[tokio::test]
async fn last_over_time_aggregate_retains_history_and_exact_window_boundaries() {
    let mut store = InMemoryMetricStore::new();
    for sample in [
        StoredSampleCase {
            instance: "a",
            job: "api",
            timestamp_ms: 1_000,
            value: 4.0,
        },
        StoredSampleCase {
            instance: "a",
            job: "api",
            timestamp_ms: 1_800_000,
            value: stale_nan(),
        },
        StoredSampleCase {
            instance: "b",
            job: "worker",
            timestamp_ms: 0,
            value: 100.0,
        },
        StoredSampleCase {
            instance: "b",
            job: "worker",
            timestamp_ms: 1_800_000,
            value: 6.0,
        },
        StoredSampleCase {
            instance: "left",
            job: "api",
            timestamp_ms: 0,
            value: 300.0,
        },
        StoredSampleCase {
            instance: "future",
            job: "api",
            timestamp_ms: 1_800_001,
            value: 500.0,
        },
    ] {
        store.push_float(
            "t",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "m"),
                crate::test_support::TestLabelPair::new("instance", sample.instance),
                crate::test_support::TestLabelPair::new("job", sample.job),
            ]),
            sample.timestamp_ms,
            sample.value,
        );
    }
    store.push_float(
        "other",
        labels(&[crate::test_support::TestLabelPair::new("__name__", "m")]),
        1_800_000,
        700.0,
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for expectation in [
        QueryExpectation {
            query: "sum(last_over_time(m[30m]))",
            timestamp_ms: 1_800_000,
            expected: vector(
                1_800_000,
                &[ExpectedVectorSample::new(PromqlLabels::new(), 10.0)],
            ),
        },
        QueryExpectation {
            query: "avg(last_over_time(m[30m]))",
            timestamp_ms: 1_800_000,
            expected: vector(
                1_800_000,
                &[ExpectedVectorSample::new(PromqlLabels::new(), 5.0)],
            ),
        },
        QueryExpectation {
            query: "sum by () (last_over_time(m[30m]))",
            timestamp_ms: 1_800_000,
            expected: vector(
                1_800_000,
                &[ExpectedVectorSample::new(PromqlLabels::new(), 10.0)],
            ),
        },
        QueryExpectation {
            query: "sum((last_over_time(m[30m])))",
            timestamp_ms: 1_800_000,
            expected: vector(
                1_800_000,
                &[ExpectedVectorSample::new(PromqlLabels::new(), 10.0)],
            ),
        },
        QueryExpectation {
            query: "sum(last_over_time(m[30m] @ 1800))",
            timestamp_ms: 1_900_000,
            expected: vector(
                1_900_000,
                &[ExpectedVectorSample::new(PromqlLabels::new(), 10.0)],
            ),
        },
        QueryExpectation {
            query: "sum(last_over_time(m[30m] offset 1s))",
            timestamp_ms: 1_801_000,
            expected: vector(
                1_801_000,
                &[ExpectedVectorSample::new(PromqlLabels::new(), 10.0)],
            ),
        },
        // The subquery retains the left-only series from its earlier step.
        QueryExpectation {
            query: "sum(last_over_time((last_over_time(m[30m]))[1m:30s]))",
            timestamp_ms: 1_800_000,
            expected: vector(
                1_800_000,
                &[ExpectedVectorSample::new(PromqlLabels::new(), 310.0)],
            ),
        },
        QueryExpectation {
            query: "sum by (job) (last_over_time(m[30m]))",
            timestamp_ms: 1_800_000,
            expected: vector(
                1_800_000,
                &[
                    ExpectedVectorSample::new(
                        PromqlLabels::from_pairs([krabka_metrics::MetricLabelPair::new(
                            "job", "api",
                        )]),
                        4.0,
                    ),
                    ExpectedVectorSample::new(
                        PromqlLabels::from_pairs([krabka_metrics::MetricLabelPair::new(
                            "job", "worker",
                        )]),
                        6.0,
                    ),
                ],
            ),
        },
        QueryExpectation {
            query: "sum(m)",
            timestamp_ms: 1_800_000,
            expected: vector(
                1_800_000,
                &[ExpectedVectorSample::new(PromqlLabels::new(), 6.0)],
            ),
        },
        QueryExpectation {
            query: "sum(last_over_time(missing[30m]))",
            timestamp_ms: 1_800_000,
            expected: vector(1_800_000, &[]),
        },
        QueryExpectation {
            query: "sum(last_over_time(m{instance=\"left\"}[30m]))",
            timestamp_ms: 1_800_000,
            expected: vector(1_800_000, &[]),
        },
    ] {
        let actual = engine
            .query_instant_with_annotations(
                &tenant_id("t"),
                expectation.query,
                expectation.timestamp_ms,
            )
            .await
            .unwrap();
        assert2::assert!(
            actual
                == AnnotatedQueryResult {
                    result: expectation.expected,
                    annotations: Annotations::default()
                },
            "{}",
            expectation.query
        );
    }
}

#[tokio::test]
async fn last_over_time_aggregate_uses_hot_ties_but_skips_hot_staleness() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    let series = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "m"),
        crate::test_support::TestLabelPair::new("instance", "a"),
    ]);
    cold.push_float("t", series.clone(), 5_000, 3.0);
    cold.push_float("t", series.clone(), 10_000, 99.0);
    hot.push_float("t", series.clone(), 10_000, 7.0);
    hot.push_float("t", series, 20_000, stale_nan());
    hot.push_float(
        "t",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "m"),
            crate::test_support::TestLabelPair::new("instance", "b"),
        ]),
        20_000,
        5.0,
    );
    let engine = PromqlEngine::new(
        Arc::new(MergedMetricStore::new(cold, WalHead::from_store(hot))),
        EngineOpts::default(),
    );
    for case in [
        QueryValueCase {
            query: "sum(last_over_time(m[30m]))",
            timestamp_ms: 10_000,
            value: 7.0,
        },
        QueryValueCase {
            query: "sum(last_over_time(m[30m]))",
            timestamp_ms: 20_000,
            value: 12.0,
        },
        QueryValueCase {
            query: "avg(last_over_time(m[30m]))",
            timestamp_ms: 20_000,
            value: 6.0,
        },
        QueryValueCase {
            query: "sum(m)",
            timestamp_ms: 20_000,
            value: 5.0,
        },
    ] {
        let actual = engine
            .query_instant_with_annotations(&tenant_id("t"), case.query, case.timestamp_ms)
            .await
            .unwrap();
        assert2::assert!(
            actual
                == AnnotatedQueryResult {
                    result: vector(
                        case.timestamp_ms,
                        &[ExpectedVectorSample::new(PromqlLabels::new(), case.value)]
                    ),
                    annotations: Annotations::default()
                },
            "{}",
            case.query
        );
    }
    for values in [[2.0, 7.0], [7.0, 2.0]] {
        let mut store = InMemoryMetricStore::new();
        for value in values {
            store.push_float(
                "t",
                labels(&[crate::test_support::TestLabelPair::new("__name__", "m")]),
                10_000,
                value,
            );
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let actual = engine
            .query_instant_with_annotations(&tenant_id("t"), "sum(last_over_time(m[30m]))", 10_000)
            .await
            .unwrap();
        assert2::assert!(
            actual
                == AnnotatedQueryResult {
                    result: vector(
                        10_000,
                        &[ExpectedVectorSample::new(PromqlLabels::new(), values[1])]
                    ),
                    annotations: Annotations::default()
                }
        );
    }
}

#[tokio::test]
async fn last_over_time_aggregate_keeps_float_bits_and_compensation() {
    for case in [
        FloatAggregateInput {
            values: vec![-0.0],
            sum: 0.0,
            average: 0.0,
        },
        FloatAggregateInput {
            values: vec![f64::INFINITY],
            sum: f64::INFINITY,
            average: f64::INFINITY,
        },
        FloatAggregateInput {
            values: vec![f64::NEG_INFINITY],
            sum: f64::NEG_INFINITY,
            average: f64::NEG_INFINITY,
        },
        FloatAggregateInput {
            values: vec![f64::NAN],
            sum: f64::NAN,
            average: f64::NAN,
        },
        FloatAggregateInput {
            values: vec![f64::INFINITY, f64::NEG_INFINITY],
            sum: f64::NAN,
            average: f64::NAN,
        },
        FloatAggregateInput {
            values: vec![1e16, 1.0, -1e16],
            sum: 1.0,
            average: 1.0 / 3.0,
        },
    ] {
        let store = instance_store(&case.values, InsertionOrder::Forward);
        let engine = PromqlEngine::new(
            Arc::new(MergedMetricStore::new(InMemoryMetricStore::new(), store)),
            EngineOpts::default(),
        );
        for query_case in [
            QueryScalarCase {
                query: "sum(last_over_time(m[30m]))",
                value: case.sum,
            },
            QueryScalarCase {
                query: "avg(last_over_time(m[30m]))",
                value: case.average,
            },
        ] {
            let mut annotated = engine
                .query_instant_with_annotations(&tenant_id("t"), query_case.query, 10_000)
                .await
                .unwrap();
            let QueryResult::InstantVector(samples) = &mut annotated.result else {
                panic!("expected vector")
            };
            assert2::assert!(samples.len() == 1);
            let SampleValue::Float(value) = &mut samples[0].value else {
                panic!("expected float")
            };
            if query_case.value.is_nan() {
                assert2::assert!(value.is_nan());
                // IEEE equality cannot compare NaNs; replace only the value after
                // proving its classification, then compare the complete result.
                *value = 0.0;
            } else {
                assert2::assert!(
                    value.to_bits() == query_case.value.to_bits(),
                    "{}: {:?}",
                    query_case.query,
                    case.values
                );
            }
            let expected = if query_case.value.is_nan() {
                0.0
            } else {
                query_case.value
            };
            assert2::assert!(
                annotated
                    == crate::AnnotatedQueryResult {
                        result: vector(
                            10_000,
                            &[ExpectedVectorSample::new(PromqlLabels::new(), expected)]
                        ),
                        annotations: Annotations::default(),
                    }
            );
        }
    }

    // Canonical fingerprint order is c,b,a. Opposite insertion orders must both
    // overflow on the two positive maxima before reaching the negative maximum.
    // Summing in insertion order a,b,c instead would produce finite MAX.
    for order in [["a", "b", "c"], ["c", "b", "a"]] {
        let mut store = InMemoryMetricStore::new();
        for instance in order {
            let value = if instance == "a" { -f64::MAX } else { f64::MAX };
            store.push_float(
                "t",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "order"),
                    crate::test_support::TestLabelPair::new("instance", instance),
                ]),
                10_000,
                value,
            );
        }
        let engine = PromqlEngine::new(
            Arc::new(MergedMetricStore::new(InMemoryMetricStore::new(), store)),
            EngineOpts::default(),
        );
        let actual = engine
            .query_instant_with_annotations(
                &tenant_id("t"),
                "sum(last_over_time(order[30m]))",
                10_000,
            )
            .await
            .unwrap();
        assert2::assert!(
            actual
                == AnnotatedQueryResult {
                    result: vector(
                        10_000,
                        &[ExpectedVectorSample::new(
                            PromqlLabels::new(),
                            f64::INFINITY
                        )]
                    ),
                    annotations: Annotations::default()
                }
        );
    }
}

#[tokio::test]
async fn last_over_time_aggregate_retains_byte_identity_and_or_union_on_the_range_grid() {
    let mut store = InMemoryMetricStore::new();
    for case in [
        ByteIdentityCase {
            identity: None,
            value: 2.0,
        },
        ByteIdentityCase {
            identity: Some(Vec::new()),
            value: 4.0,
        },
        ByteIdentityCase {
            identity: Some(vec![0xff]),
            value: 6.0,
        },
        ByteIdentityCase {
            identity: Some(vec![0xfe]),
            value: 8.0,
        },
    ] {
        let mut series = PromqlLabels::from_pairs([
            krabka_metrics::MetricLabelPair::new("__name__", "m"),
            krabka_metrics::MetricLabelPair::new("job", "api"),
        ]);
        if let Some(bytes) = case.identity {
            series.insert("identity", crate::PromqlString::from(bytes));
        }
        for timestamp in [10_000, 20_000, 30_000] {
            store.push_float("t", series.clone(), timestamp, case.value);
        }
    }
    for case in [
        OptionalMetricNameCase {
            name: None,
            value: 1.0,
        },
        OptionalMetricNameCase {
            name: Some(""),
            value: 3.0,
        },
    ] {
        let mut series =
            PromqlLabels::from_pairs([krabka_metrics::MetricLabelPair::new("job", "hidden")]);
        if let Some(name) = case.name {
            series.insert("__name__", name);
        }
        store.push_float("t", series, 10_000, case.value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for case in [
        QueryScalarCase {
            query: "sum(last_over_time(m[30m]))",
            value: 20.0,
        },
        QueryScalarCase {
            query: "avg(last_over_time(m[30m]))",
            value: 5.0,
        },
        QueryScalarCase {
            query: r#"sum(last_over_time(m{job="api" or job=~"a.*"}[30m]))"#,
            value: 20.0,
        },
        QueryScalarCase {
            query: r#"avg(last_over_time(m{job="api" or job=~"a.*"}[30m]))"#,
            value: 5.0,
        },
    ] {
        for timestamp in [10_000, 20_000, 30_000] {
            let actual = engine
                .query_instant_with_annotations(&tenant_id("t"), case.query, timestamp)
                .await
                .unwrap();
            assert2::assert!(
                actual
                    == AnnotatedQueryResult {
                        result: vector(
                            timestamp,
                            &[ExpectedVectorSample::new(PromqlLabels::new(), case.value)]
                        ),
                        annotations: Annotations::default()
                    }
            );
        }
        let expected = QueryResult::RangeMatrix(vec![RangeSeries {
            labels: PromqlLabels::new(),
            drop_name: false,
            start_timestamps_ms: BTreeMap::new(),
            samples: vec![
                crate::RangeSeriesSample::new(10_000, SampleValue::Float(case.value)),
                crate::RangeSeriesSample::new(20_000, SampleValue::Float(case.value)),
                crate::RangeSeriesSample::new(30_000, SampleValue::Float(case.value)),
            ],
        }]);
        let actual = engine
            .query_range_with_annotations(&tenant_id("t"), case.query, 10_000, 30_000, secs(10))
            .await
            .unwrap();
        assert2::assert!(
            actual
                == AnnotatedQueryResult {
                    result: expected,
                    annotations: Annotations::default()
                }
        );
    }
    for case in [
        QueryScalarCase {
            query: r#"sum(last_over_time({job="hidden"}[30m]))"#,
            value: 4.0,
        },
        QueryScalarCase {
            query: r#"avg(last_over_time({job="hidden"}[30m]))"#,
            value: 2.0,
        },
    ] {
        let actual = engine
            .query_instant_with_annotations(&tenant_id("t"), case.query, 10_000)
            .await
            .unwrap();
        assert2::assert!(
            actual
                == AnnotatedQueryResult {
                    result: vector(
                        10_000,
                        &[ExpectedVectorSample::new(PromqlLabels::new(), case.value)]
                    ),
                    annotations: Annotations::default()
                }
        );
    }
}

#[tokio::test]
async fn last_over_time_aggregate_preserves_histogram_results_and_mixed_warnings() {
    let mut histograms = InMemoryMetricStore::new();
    for case in [
        HistogramValueCase {
            instance: "a",
            count: 2.0,
            sum: 4.0,
        },
        HistogramValueCase {
            instance: "b",
            count: 6.0,
            sum: 12.0,
        },
    ] {
        histograms.push_histogram(
            "t",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "h"),
                crate::test_support::TestLabelPair::new("instance", case.instance),
            ]),
            10_000,
            native_histogram(case.count, case.sum),
        );
    }
    let engine = PromqlEngine::new(Arc::new(histograms), EngineOpts::default());
    for case in [
        HistogramQueryCase {
            query: "sum(last_over_time(h[30m]))",
            expected: native_histogram(8.0, 16.0),
        },
        HistogramQueryCase {
            query: "avg(last_over_time(h[30m]))",
            expected: native_histogram(4.0, 8.0),
        },
    ] {
        let expected = QueryResult::InstantVector(vec![InstantSample {
            labels: PromqlLabels::new(),
            ts_ms: 10_000,
            value: SampleValue::Histogram(case.expected),
            drop_name: false,
        }]);
        let actual = engine
            .query_instant_with_annotations(&tenant_id("t"), case.query, 10_000)
            .await
            .unwrap();
        assert2::assert!(
            actual
                == AnnotatedQueryResult {
                    result: expected,
                    annotations: Annotations::default()
                }
        );
    }

    let mut mixed = InMemoryMetricStore::new();
    mixed.push_histogram(
        "t",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "h"),
            crate::test_support::TestLabelPair::new("instance", "hist"),
        ]),
        10_000,
        native_histogram(2.0, 4.0),
    );
    mixed.push_float(
        "t",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "h"),
            crate::test_support::TestLabelPair::new("instance", "float"),
        ]),
        10_000,
        7.0,
    );
    let engine = PromqlEngine::new(Arc::new(mixed), EngineOpts::default());
    for query in ["sum(last_over_time(h[30m]))", "avg(last_over_time(h[30m]))"] {
        let annotations = Annotations {
            warnings: vec![
                "PromQL warning: encountered a mix of histograms and floats for aggregation (1:5)"
                    .to_owned(),
            ],
            ..Annotations::default()
        };
        let actual = engine
            .query_instant_with_annotations(&tenant_id("t"), query, 10_000)
            .await
            .unwrap();
        assert2::assert!(
            actual
                == AnnotatedQueryResult {
                    result: vector(10_000, &[]),
                    annotations: annotations
                }
        );
    }
}

#[tokio::test]
async fn last_over_time_aggregate_counts_stale_unresolved_and_boundary_only_series_for_limits() {
    let mut store = InMemoryMetricStore::new();
    for sample in [
        InstanceSampleCase {
            instance: "a",
            timestamp_ms: 1_000,
            value: 3.0,
        },
        InstanceSampleCase {
            instance: "a",
            timestamp_ms: 2_000,
            value: stale_nan(),
        },
        InstanceSampleCase {
            instance: "a",
            timestamp_ms: 2_500,
            value: stale_nan(),
        },
        InstanceSampleCase {
            instance: "left",
            timestamp_ms: 0,
            value: 100.0,
        },
        InstanceSampleCase {
            instance: "b",
            timestamp_ms: 1_000,
            value: 7.0,
        },
    ] {
        store.push_float(
            "t",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "m"),
                crate::test_support::TestLabelPair::new("instance", sample.instance),
            ]),
            sample.timestamp_ms,
            sample.value,
        );
    }
    // A row identity with no canonical-label-map entry is omitted from the
    // answer, but still belongs to the fetched-row budget.
    let mut orphan = store.floats["t"].iter().next().unwrap().clone();
    orphan.fp = 0;
    orphan.labels = Arc::new(PromqlLabels::from_pairs([
        krabka_metrics::MetricLabelPair::new("__name__", "m"),
        krabka_metrics::MetricLabelPair::new("instance", "orphan"),
    ]));
    orphan.ts_ms = 1_000;
    orphan.value = 9.0;
    store.floats.get_mut("t").unwrap().push(orphan);
    for query in ["sum(last_over_time(m[3s]))", "avg(last_over_time(m[3s]))"] {
        for budget in [
            LimitBudgetCase {
                max_samples: 4,
                max_fetched_series: 0,
                error: Some(LimitError::SamplesPerQueryExceeded {
                    limit: 4,
                    observed: 5,
                }),
            },
            LimitBudgetCase {
                max_samples: 5,
                max_fetched_series: 3,
                error: Some(LimitError::SeriesPerQueryExceeded {
                    limit: 3,
                    observed: 4,
                }),
            },
            LimitBudgetCase {
                max_samples: 5,
                max_fetched_series: 4,
                error: None,
            },
            LimitBudgetCase {
                max_samples: 5,
                max_fetched_series: 0,
                error: None,
            },
        ] {
            let engine = PromqlEngine::new(
                Arc::new(store.clone()),
                EngineOpts {
                    max_samples: budget.max_samples,
                    max_fetched_series: budget.max_fetched_series,
                    ..EngineOpts::default()
                },
            );
            let actual = engine
                .query_instant_with_annotations(&tenant_id("t"), query, 3_000)
                .await;
            if let Some(expected) = budget.error {
                assert2::assert!(
                    matches!(actual, Err(PromqlError::Limit(got)) if got == expected),
                    "{query}, {}, {}",
                    budget.max_samples,
                    budget.max_fetched_series
                );
            } else {
                let value = if query.starts_with("sum") { 10.0 } else { 5.0 };
                assert2::assert!(
                    actual.unwrap()
                        == AnnotatedQueryResult {
                            result: vector(
                                3_000,
                                &[ExpectedVectorSample::new(PromqlLabels::new(), value)]
                            ),
                            annotations: Annotations::default()
                        }
                );
            }
        }
    }
}

#[tokio::test]
async fn last_over_time_aggregate_keeps_reserved_column_errors() {
    for name in [
        "timestamp",
        "value",
        "timestamp_range",
        "value_range",
        "UPPER",
        "scope.field",
        "\"quoted\"",
    ] {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "t",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "m"),
                crate::test_support::TestLabelPair::new(name, "reserved"),
            ]),
            10_000,
            7.0,
        );
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        // The child operator rejects its reserved column name. An outer global
        // aggregation must retain that failure instead of bypassing the child.
        let expected = engine
            .query_instant(&tenant_id("t"), "last_over_time(m[30m])", 10_000)
            .await
            .unwrap_err()
            .to_string();
        for query in ["sum(last_over_time(m[30m]))", "avg(last_over_time(m[30m]))"] {
            let actual = engine
                .query_instant(&tenant_id("t"), query, 10_000)
                .await
                .unwrap_err()
                .to_string();
            assert2::assert!(actual == expected, "{name}: {query}");
        }
    }
}

#[tokio::test]
async fn last_over_time_aggregate_keeps_timestamp_boundary_errors() {
    for case in [
        TimestampRangeErrorCase {
            timestamp_ms: i64::MAX,
            range: "1ms",
            message: "grid timestamp overflow",
        },
        TimestampRangeErrorCase {
            timestamp_ms: i64::MIN + 1,
            range: "2ms",
            message: "range lower-bound underflow",
        },
    ] {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "t",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "m")]),
            case.timestamp_ms,
            7.0,
        );
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let child = format!("last_over_time(m[{}])", case.range);
        let expected = engine
            .query_instant(&tenant_id("t"), &child, case.timestamp_ms)
            .await
            .unwrap_err()
            .to_string();
        assert2::assert!(expected.contains(case.message), "{expected}");
        for op in ["sum", "avg"] {
            let query = format!("{op}({child})");
            let actual = engine
                .query_instant(&tenant_id("t"), &query, case.timestamp_ms)
                .await
                .unwrap_err()
                .to_string();
            assert2::assert!(actual == expected, "{query}");
        }
    }
}

#[tokio::test]
async fn last_over_time_aggregate_accepts_nonreserved_and_unicode_label_names() {
    for name in ["sample_timestamp", "区域", "slot1"] {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "t",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "m"),
                crate::test_support::TestLabelPair::new(name, "kept"),
            ]),
            10_000,
            7.0,
        );
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for query in ["sum(last_over_time(m[30m]))", "avg(last_over_time(m[30m]))"] {
            let actual = engine
                .query_instant_with_annotations(&tenant_id("t"), query, 10_000)
                .await
                .unwrap();
            assert2::assert!(
                actual
                    == AnnotatedQueryResult {
                        result: vector(
                            10_000,
                            &[ExpectedVectorSample::new(PromqlLabels::new(), 7.0)]
                        ),
                        annotations: Annotations::default()
                    },
                "{name}: {query}"
            );
        }
    }
}

/// What a one-series aggregate answers at 2ms under a two-sample cap.
#[derive(Clone, Copy)]
enum CappedOutcome {
    /// The aggregate fits the cap and answers this unnamed value.
    Value(f64),
    /// The query reads three samples and exceeds the cap of two.
    SamplesPerQueryExceeded,
}

/// Checks that `query` for tenant `t` at 2ms answers `outcome`.
async fn check_capped_aggregate<S: crate::MetricStore>(
    engine: &PromqlEngine<S>,
    query: &str,
    outcome: CappedOutcome,
) {
    let actual = engine
        .query_instant_with_annotations(&tenant_id("t"), query, 2)
        .await;
    match outcome {
        CappedOutcome::Value(value) => assert2::assert!(
            actual.unwrap()
                == AnnotatedQueryResult {
                    result: vector(2, &[ExpectedVectorSample::new(PromqlLabels::new(), value)]),
                    annotations: Annotations::default()
                }
        ),
        CappedOutcome::SamplesPerQueryExceeded => assert2::assert!(matches!(
            actual,
            Err(PromqlError::Limit(LimitError::SamplesPerQueryExceeded {
                limit: 2,
                observed: 3
            }))
        )),
    }
}

#[tokio::test]
async fn last_over_time_aggregate_preserves_duplicate_stale_history_and_raw_caps() {
    let mut hot = InMemoryMetricStore::new();
    let series = labels(&[crate::test_support::TestLabelPair::new("__name__", "m")]);
    for sample in [
        TimestampValueCase {
            timestamp_ms: 2,
            value: 9.0,
        },
        TimestampValueCase {
            timestamp_ms: 1,
            value: 5.0,
        },
        TimestampValueCase {
            timestamp_ms: 2,
            value: stale_nan(),
        },
    ] {
        hot.push_float("t", series.clone(), sample.timestamp_ms, sample.value);
    }
    // A generic scan retains its last duplicate, including a stale marker.
    // The earlier non-stale sample then answers last_over_time.
    for case in [
        OptionalFloatCase {
            max_samples: 3,
            expected: Some(5.0),
        },
        OptionalFloatCase {
            max_samples: 2,
            expected: None,
        },
    ] {
        let engine = PromqlEngine::new(
            Arc::new(hot.clone()),
            EngineOpts {
                max_samples: case.max_samples,
                ..EngineOpts::default()
            },
        );
        for query in ["sum(last_over_time(m[2ms]))", "avg(last_over_time(m[2ms]))"] {
            let outcome = case
                .expected
                .map_or(CappedOutcome::SamplesPerQueryExceeded, CappedOutcome::Value);
            check_capped_aggregate(&engine, query, outcome).await;
        }
    }
    // The production merged scan chooses the first hot duplicate, and its
    // stale cold duplicate cannot replace that sample.
    let mut cold = InMemoryMetricStore::new();
    cold.push_float("t", series, 2, stale_nan());
    cold.push_float(
        "t",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "m"),
            crate::test_support::TestLabelPair::new("instance", "cold"),
        ]),
        2,
        4.0,
    );
    for case in [
        AcceptedBudgetCase {
            max_samples: 3,
            accepted: true,
        },
        AcceptedBudgetCase {
            max_samples: 2,
            accepted: false,
        },
    ] {
        let engine = PromqlEngine::new(
            Arc::new(MergedMetricStore::new(cold.clone(), hot.clone())),
            EngineOpts {
                max_samples: case.max_samples,
                ..EngineOpts::default()
            },
        );
        for query_case in [
            QueryScalarCase {
                query: "sum(last_over_time(m[2ms]))",
                value: 13.0,
            },
            QueryScalarCase {
                query: "avg(last_over_time(m[2ms]))",
                value: 6.5,
            },
        ] {
            let outcome = if case.accepted {
                CappedOutcome::Value(query_case.value)
            } else {
                CappedOutcome::SamplesPerQueryExceeded
            };
            check_capped_aggregate(&engine, query_case.query, outcome).await;
        }
    }
}

#[tokio::test]
async fn last_over_time_aggregate_reuses_wider_scans_and_initialized_float_windows() {
    let mut hot = InMemoryMetricStore::new();
    for sample in [
        TimestampValueCase {
            timestamp_ms: 1,
            value: 5.0,
        },
        TimestampValueCase {
            timestamp_ms: 2,
            value: 9.0,
        },
        TimestampValueCase {
            timestamp_ms: 3,
            value: stale_nan(),
        },
    ] {
        hot.push_float(
            "t",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "m")]),
            sample.timestamp_ms,
            sample.value,
        );
    }
    let engine = PromqlEngine::new(
        Arc::new(MergedMetricStore::new(InMemoryMetricStore::new(), hot)),
        EngineOpts::default(),
    );
    for case in [
        QueryStatsCase {
            query: "sum(last_over_time(m[3ms])) + sum(last_over_time(m[2ms]))",
            value: 18.0,
            total: 5,
            peak: 3,
        },
        QueryStatsCase {
            query: "sum(sum_over_time(m[3ms])) + sum(last_over_time(m[3ms]))",
            value: 23.0,
            total: 6,
            peak: 3,
        },
        QueryStatsCase {
            query: "sum(last_over_time(m[3ms])) + sum(sum_over_time(m[3ms]))",
            value: 23.0,
            total: 6,
            peak: 3,
        },
    ] {
        let query_result = super::super::collect_query_sample_stats(
            super::super::PerStepSampleStats::Omit,
            engine.query_instant_with_annotations(&tenant_id("t"), case.query, 3),
        )
        .await;
        assert2::assert!(
            query_result.output.unwrap()
                == AnnotatedQueryResult {
                    result: vector(
                        3,
                        &[ExpectedVectorSample::new(PromqlLabels::new(), case.value)]
                    ),
                    annotations: Annotations::default()
                },
            "{}",
            case.query
        );
        assert2::assert!(
            query_result.stats
                == super::super::QuerySampleStats {
                    total_queryable_samples: case.total,
                    peak_samples: case.peak,
                    per_step: BTreeMap::new(),
                },
            "{}",
            case.query
        );
    }
}
