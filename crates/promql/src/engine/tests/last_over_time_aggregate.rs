use krabka_metrics::LimitError;

use super::{
    super::{
        aggregation::{AggregateOp, AggregateState},
        range_functions::{over_time_mean, over_time_sum},
    },
    *,
};
use crate::{Annotations, InstantSample, MergedMetricStore, PromqlLabels, RangeSeries, WalHead};

fn vector(time_ms: i64, rows: &[(&[(&str, &str)], f64)]) -> QueryResult {
    QueryResult::InstantVector(
        rows.iter()
            .map(|(pairs, value)| InstantSample {
                labels: PromqlLabels::from_pairs(pairs.iter().copied()),
                ts_ms: time_ms,
                value: SampleValue::Float(*value),
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
    for (index, sample_value) in indexed {
        store.push_float(
            "t",
            labels(&[("__name__", "m"), ("instance", &index.to_string())]),
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
    let (actual, annotations) = engine
        .query_instant_with_annotations(&tenant_id("t"), query, 10_000)
        .await
        .unwrap();
    let expected = engine.eval_instant_expr("t", &expr, 10_000).await.unwrap();
    let QueryResult::InstantVector(mut actual) = actual else {
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
    assert2::assert!((actual, annotations) == (expected, Annotations::default()));
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
        for (op, actual) in [
            (AggregateOp::Sum, over_time_sum(values.iter().copied())),
            (AggregateOp::Avg, over_time_mean(values.iter().copied())),
        ] {
            let Some(SampleValue::Float(expected)) = op.finish(&state) else {
                panic!("expected float")
            };
            assert2::assert!(actual.to_bits() == expected.to_bits(), "{values:?}");
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
        store.push_float("t", labels(&[("__name__", "m")]), 1_000, value);
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
        let (total, rows) = engine
            .last_float_rows("t", &matchers, 1, 3_000)
            .await
            .unwrap();
        let raw_rows = rows
            .iter()
            .map(|row| {
                (
                    row.fp,
                    row.ts_ms,
                    row.value.to_bits(),
                    row.start_timestamp_ms,
                )
            })
            .collect::<Vec<_>>();
        assert2::assert!((total, raw_rows) == (1, vec![(0, 1_000, value.to_bits(), None)]));
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
            assert2::assert!(actual == (vector(3_000, &[]), Annotations::default()));
        }
    }
}

#[tokio::test]
async fn last_over_time_aggregate_retains_history_and_exact_window_boundaries() {
    let mut store = InMemoryMetricStore::new();
    for (instance, job, timestamp, value) in [
        ("a", "api", 1_000, 4.0),
        ("a", "api", 1_800_000, stale_nan()),
        ("b", "worker", 0, 100.0),
        ("b", "worker", 1_800_000, 6.0),
        ("left", "api", 0, 300.0),
        ("future", "api", 1_800_001, 500.0),
    ] {
        store.push_float(
            "t",
            labels(&[("__name__", "m"), ("instance", instance), ("job", job)]),
            timestamp,
            value,
        );
    }
    store.push_float("other", labels(&[("__name__", "m")]), 1_800_000, 700.0);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, time_ms, expected) in [
        (
            "sum(last_over_time(m[30m]))",
            1_800_000,
            vector(1_800_000, &[(&[], 10.0)]),
        ),
        (
            "avg(last_over_time(m[30m]))",
            1_800_000,
            vector(1_800_000, &[(&[], 5.0)]),
        ),
        (
            "sum by () (last_over_time(m[30m]))",
            1_800_000,
            vector(1_800_000, &[(&[], 10.0)]),
        ),
        (
            "sum((last_over_time(m[30m])))",
            1_800_000,
            vector(1_800_000, &[(&[], 10.0)]),
        ),
        (
            "sum(last_over_time(m[30m] @ 1800))",
            1_900_000,
            vector(1_900_000, &[(&[], 10.0)]),
        ),
        (
            "sum(last_over_time(m[30m] offset 1s))",
            1_801_000,
            vector(1_801_000, &[(&[], 10.0)]),
        ),
        // The subquery retains the left-only series from its earlier step.
        (
            "sum(last_over_time((last_over_time(m[30m]))[1m:30s]))",
            1_800_000,
            vector(1_800_000, &[(&[], 310.0)]),
        ),
        (
            "sum by (job) (last_over_time(m[30m]))",
            1_800_000,
            vector(
                1_800_000,
                &[(&[("job", "api")], 4.0), (&[("job", "worker")], 6.0)],
            ),
        ),
        ("sum(m)", 1_800_000, vector(1_800_000, &[(&[], 6.0)])),
        (
            "sum(last_over_time(missing[30m]))",
            1_800_000,
            vector(1_800_000, &[]),
        ),
        (
            "sum(last_over_time(m{instance=\"left\"}[30m]))",
            1_800_000,
            vector(1_800_000, &[]),
        ),
    ] {
        let actual = engine
            .query_instant_with_annotations(&tenant_id("t"), query, time_ms)
            .await
            .unwrap();
        assert2::assert!(actual == (expected, Annotations::default()), "{query}");
    }
}

#[tokio::test]
async fn last_over_time_aggregate_uses_hot_ties_but_skips_hot_staleness() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    let series = labels(&[("__name__", "m"), ("instance", "a")]);
    cold.push_float("t", series.clone(), 5_000, 3.0);
    cold.push_float("t", series.clone(), 10_000, 99.0);
    hot.push_float("t", series.clone(), 10_000, 7.0);
    hot.push_float("t", series, 20_000, stale_nan());
    hot.push_float(
        "t",
        labels(&[("__name__", "m"), ("instance", "b")]),
        20_000,
        5.0,
    );
    let engine = PromqlEngine::new(
        Arc::new(MergedMetricStore::new(cold, WalHead::from_store(hot))),
        EngineOpts::default(),
    );
    for (query, time_ms, value) in [
        ("sum(last_over_time(m[30m]))", 10_000, 7.0),
        ("sum(last_over_time(m[30m]))", 20_000, 12.0),
        ("avg(last_over_time(m[30m]))", 20_000, 6.0),
        ("sum(m)", 20_000, 5.0),
    ] {
        let actual = engine
            .query_instant_with_annotations(&tenant_id("t"), query, time_ms)
            .await
            .unwrap();
        assert2::assert!(
            actual == (vector(time_ms, &[(&[], value)]), Annotations::default()),
            "{query}"
        );
    }
    for values in [[2.0, 7.0], [7.0, 2.0]] {
        let mut store = InMemoryMetricStore::new();
        for value in values {
            store.push_float("t", labels(&[("__name__", "m")]), 10_000, value);
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let actual = engine
            .query_instant_with_annotations(&tenant_id("t"), "sum(last_over_time(m[30m]))", 10_000)
            .await
            .unwrap();
        assert2::assert!(actual == (vector(10_000, &[(&[], values[1])]), Annotations::default()));
    }
}

#[tokio::test]
async fn last_over_time_aggregate_keeps_float_bits_and_compensation() {
    for (values, sum, avg) in [
        (vec![-0.0], 0.0, 0.0),
        (vec![f64::INFINITY], f64::INFINITY, f64::INFINITY),
        (
            vec![f64::NEG_INFINITY],
            f64::NEG_INFINITY,
            f64::NEG_INFINITY,
        ),
        (vec![f64::NAN], f64::NAN, f64::NAN),
        (vec![f64::INFINITY, f64::NEG_INFINITY], f64::NAN, f64::NAN),
        (vec![1e16, 1.0, -1e16], 1.0, 1.0 / 3.0),
    ] {
        let store = instance_store(&values, InsertionOrder::Forward);
        let engine = PromqlEngine::new(
            Arc::new(MergedMetricStore::new(InMemoryMetricStore::new(), store)),
            EngineOpts::default(),
        );
        for (query, expected) in [
            ("sum(last_over_time(m[30m]))", sum),
            ("avg(last_over_time(m[30m]))", avg),
        ] {
            let (mut result, annotations) = engine
                .query_instant_with_annotations(&tenant_id("t"), query, 10_000)
                .await
                .unwrap();
            let QueryResult::InstantVector(samples) = &mut result else {
                panic!("expected vector")
            };
            assert2::assert!(samples.len() == 1);
            let SampleValue::Float(value) = &mut samples[0].value else {
                panic!("expected float")
            };
            if expected.is_nan() {
                assert2::assert!(value.is_nan());
                // IEEE equality cannot compare NaNs; replace only the value after
                // proving its classification, then compare the complete result.
                *value = 0.0;
            } else {
                assert2::assert!(value.to_bits() == expected.to_bits(), "{query}: {values:?}");
            }
            let expected = if expected.is_nan() { 0.0 } else { expected };
            assert2::assert!(
                (result, annotations)
                    == (vector(10_000, &[(&[], expected)]), Annotations::default())
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
                labels(&[("__name__", "order"), ("instance", instance)]),
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
                == (
                    vector(10_000, &[(&[], f64::INFINITY)]),
                    Annotations::default()
                )
        );
    }
}

#[tokio::test]
async fn last_over_time_aggregate_retains_byte_identity_and_or_union_on_the_range_grid() {
    let mut store = InMemoryMetricStore::new();
    for (extra, value) in [
        (None, 2.0),
        (Some(Vec::new()), 4.0),
        (Some(vec![0xff]), 6.0),
        (Some(vec![0xfe]), 8.0),
    ] {
        let mut series = PromqlLabels::from_pairs([("__name__", "m"), ("job", "api")]);
        if let Some(bytes) = extra {
            series.insert("identity", crate::PromqlString::from(bytes));
        }
        for timestamp in [10_000, 20_000, 30_000] {
            store.push_float("t", series.clone(), timestamp, value);
        }
    }
    for (name, value) in [(None, 1.0), (Some(""), 3.0)] {
        let mut series = PromqlLabels::from_pairs([("job", "hidden")]);
        if let Some(name) = name {
            series.insert("__name__", name);
        }
        store.push_float("t", series, 10_000, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, value) in [
        ("sum(last_over_time(m[30m]))", 20.0),
        ("avg(last_over_time(m[30m]))", 5.0),
        (
            r#"sum(last_over_time(m{job="api" or job=~"a.*"}[30m]))"#,
            20.0,
        ),
        (
            r#"avg(last_over_time(m{job="api" or job=~"a.*"}[30m]))"#,
            5.0,
        ),
    ] {
        for timestamp in [10_000, 20_000, 30_000] {
            let actual = engine
                .query_instant_with_annotations(&tenant_id("t"), query, timestamp)
                .await
                .unwrap();
            assert2::assert!(
                actual == (vector(timestamp, &[(&[], value)]), Annotations::default())
            );
        }
        let expected = QueryResult::RangeMatrix(vec![RangeSeries {
            labels: PromqlLabels::new(),
            drop_name: false,
            start_timestamps_ms: BTreeMap::new(),
            samples: vec![
                (10_000, SampleValue::Float(value)),
                (20_000, SampleValue::Float(value)),
                (30_000, SampleValue::Float(value)),
            ],
        }]);
        let actual = engine
            .query_range_with_annotations(&tenant_id("t"), query, 10_000, 30_000, secs(10))
            .await
            .unwrap();
        assert2::assert!(actual == (expected, Annotations::default()));
    }
    for (query, value) in [
        (r#"sum(last_over_time({job="hidden"}[30m]))"#, 4.0),
        (r#"avg(last_over_time({job="hidden"}[30m]))"#, 2.0),
    ] {
        let actual = engine
            .query_instant_with_annotations(&tenant_id("t"), query, 10_000)
            .await
            .unwrap();
        assert2::assert!(actual == (vector(10_000, &[(&[], value)]), Annotations::default()));
    }
}

#[tokio::test]
async fn last_over_time_aggregate_preserves_histogram_results_and_mixed_warnings() {
    let mut histograms = InMemoryMetricStore::new();
    for (instance, count, sum) in [("a", 2.0, 4.0), ("b", 6.0, 12.0)] {
        histograms.push_histogram(
            "t",
            labels(&[("__name__", "h"), ("instance", instance)]),
            10_000,
            native_histogram(count, sum),
        );
    }
    let engine = PromqlEngine::new(Arc::new(histograms), EngineOpts::default());
    for (query, expected) in [
        ("sum(last_over_time(h[30m]))", native_histogram(8.0, 16.0)),
        ("avg(last_over_time(h[30m]))", native_histogram(4.0, 8.0)),
    ] {
        let expected = QueryResult::InstantVector(vec![InstantSample {
            labels: PromqlLabels::new(),
            ts_ms: 10_000,
            value: SampleValue::Histogram(expected),
            drop_name: false,
        }]);
        let actual = engine
            .query_instant_with_annotations(&tenant_id("t"), query, 10_000)
            .await
            .unwrap();
        assert2::assert!(actual == (expected, Annotations::default()));
    }

    let mut mixed = InMemoryMetricStore::new();
    mixed.push_histogram(
        "t",
        labels(&[("__name__", "h"), ("instance", "hist")]),
        10_000,
        native_histogram(2.0, 4.0),
    );
    mixed.push_float(
        "t",
        labels(&[("__name__", "h"), ("instance", "float")]),
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
        assert2::assert!(actual == (vector(10_000, &[]), annotations));
    }
}

#[tokio::test]
async fn last_over_time_aggregate_counts_stale_unresolved_and_boundary_only_series_for_limits() {
    let mut store = InMemoryMetricStore::new();
    for (instance, timestamp, value) in [
        ("a", 1_000, 3.0),
        ("a", 2_000, stale_nan()),
        ("a", 2_500, stale_nan()),
        ("left", 0, 100.0),
        ("b", 1_000, 7.0),
    ] {
        store.push_float(
            "t",
            labels(&[("__name__", "m"), ("instance", instance)]),
            timestamp,
            value,
        );
    }
    // A row identity with no canonical-label-map entry is omitted from the
    // answer, but still belongs to the fetched-row budget.
    let mut orphan = store.floats["t"].iter().next().unwrap().clone();
    orphan.fp = 0;
    orphan.labels = Arc::new(PromqlLabels::from_pairs([
        ("__name__", "m"),
        ("instance", "orphan"),
    ]));
    orphan.ts_ms = 1_000;
    orphan.value = 9.0;
    store.floats.get_mut("t").unwrap().push(orphan);
    for query in ["sum(last_over_time(m[3s]))", "avg(last_over_time(m[3s]))"] {
        for (max_samples, max_fetched_series, error) in [
            (
                4,
                0,
                Some(LimitError::SamplesPerQueryExceeded {
                    limit: 4,
                    observed: 5,
                }),
            ),
            (
                5,
                3,
                Some(LimitError::SeriesPerQueryExceeded {
                    limit: 3,
                    observed: 4,
                }),
            ),
            (5, 4, None),
            (5, 0, None),
        ] {
            let engine = PromqlEngine::new(
                Arc::new(store.clone()),
                EngineOpts {
                    max_samples,
                    max_fetched_series,
                    ..EngineOpts::default()
                },
            );
            let actual = engine
                .query_instant_with_annotations(&tenant_id("t"), query, 3_000)
                .await;
            if let Some(expected) = error {
                assert2::assert!(
                    matches!(actual, Err(PromqlError::Limit(got)) if got == expected),
                    "{query}, {max_samples}, {max_fetched_series}"
                );
            } else {
                let value = if query.starts_with("sum") { 10.0 } else { 5.0 };
                assert2::assert!(
                    actual.unwrap() == (vector(3_000, &[(&[], value)]), Annotations::default())
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
            labels(&[("__name__", "m"), (name, "reserved")]),
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
    for (time_ms, range, message) in [
        (i64::MAX, "1ms", "grid timestamp overflow"),
        (i64::MIN + 1, "2ms", "range lower-bound underflow"),
    ] {
        let mut store = InMemoryMetricStore::new();
        store.push_float("t", labels(&[("__name__", "m")]), time_ms, 7.0);
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let child = format!("last_over_time(m[{range}])");
        let expected = engine
            .query_instant(&tenant_id("t"), &child, time_ms)
            .await
            .unwrap_err()
            .to_string();
        assert2::assert!(expected.contains(message), "{expected}");
        for op in ["sum", "avg"] {
            let query = format!("{op}({child})");
            let actual = engine
                .query_instant(&tenant_id("t"), &query, time_ms)
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
            labels(&[("__name__", "m"), (name, "kept")]),
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
                actual == (vector(10_000, &[(&[], 7.0)]), Annotations::default()),
                "{name}: {query}"
            );
        }
    }
}

#[tokio::test]
async fn last_over_time_aggregate_preserves_duplicate_stale_history_and_raw_caps() {
    let mut hot = InMemoryMetricStore::new();
    let series = labels(&[("__name__", "m")]);
    for (timestamp, value) in [(2, 9.0), (1, 5.0), (2, stale_nan())] {
        hot.push_float("t", series.clone(), timestamp, value);
    }
    // A generic scan retains its last duplicate, including a stale marker.
    // The earlier non-stale sample then answers last_over_time.
    for (max_samples, expected) in [(3, Some(5.0)), (2, None)] {
        let engine = PromqlEngine::new(
            Arc::new(hot.clone()),
            EngineOpts {
                max_samples,
                ..EngineOpts::default()
            },
        );
        for query in ["sum(last_over_time(m[2ms]))", "avg(last_over_time(m[2ms]))"] {
            let actual = engine
                .query_instant_with_annotations(&tenant_id("t"), query, 2)
                .await;
            if let Some(value) = expected {
                assert2::assert!(
                    actual.unwrap() == (vector(2, &[(&[], value)]), Annotations::default())
                );
            } else {
                assert2::assert!(matches!(
                    actual,
                    Err(PromqlError::Limit(LimitError::SamplesPerQueryExceeded {
                        limit: 2,
                        observed: 3
                    }))
                ));
            }
        }
    }
    // The production merged scan chooses the first hot duplicate, and its
    // stale cold duplicate cannot replace that sample.
    let mut cold = InMemoryMetricStore::new();
    cold.push_float("t", series, 2, stale_nan());
    cold.push_float(
        "t",
        labels(&[("__name__", "m"), ("instance", "cold")]),
        2,
        4.0,
    );
    for (max_samples, accepted) in [(3, true), (2, false)] {
        let engine = PromqlEngine::new(
            Arc::new(MergedMetricStore::new(cold.clone(), hot.clone())),
            EngineOpts {
                max_samples,
                ..EngineOpts::default()
            },
        );
        for (query, value) in [
            ("sum(last_over_time(m[2ms]))", 13.0),
            ("avg(last_over_time(m[2ms]))", 6.5),
        ] {
            let actual = engine
                .query_instant_with_annotations(&tenant_id("t"), query, 2)
                .await;
            if accepted {
                assert2::assert!(
                    actual.unwrap() == (vector(2, &[(&[], value)]), Annotations::default())
                );
            } else {
                assert2::assert!(matches!(
                    actual,
                    Err(PromqlError::Limit(LimitError::SamplesPerQueryExceeded {
                        limit: 2,
                        observed: 3
                    }))
                ));
            }
        }
    }
}

#[tokio::test]
async fn last_over_time_aggregate_reuses_wider_scans_and_initialized_float_windows() {
    let mut hot = InMemoryMetricStore::new();
    for (timestamp, value) in [(1, 5.0), (2, 9.0), (3, stale_nan())] {
        hot.push_float("t", labels(&[("__name__", "m")]), timestamp, value);
    }
    let engine = PromqlEngine::new(
        Arc::new(MergedMetricStore::new(InMemoryMetricStore::new(), hot)),
        EngineOpts::default(),
    );
    for (query, value, total, peak) in [
        (
            "sum(last_over_time(m[3ms])) + sum(last_over_time(m[2ms]))",
            18.0,
            5,
            3,
        ),
        (
            "sum(sum_over_time(m[3ms])) + sum(last_over_time(m[3ms]))",
            23.0,
            6,
            3,
        ),
        (
            "sum(last_over_time(m[3ms])) + sum(sum_over_time(m[3ms]))",
            23.0,
            6,
            3,
        ),
    ] {
        let (actual, stats) = super::super::collect_query_sample_stats(
            false,
            3,
            3,
            1,
            engine.query_instant_with_annotations(&tenant_id("t"), query, 3),
        )
        .await;
        assert2::assert!(
            actual.unwrap() == (vector(3, &[(&[], value)]), Annotations::default()),
            "{query}"
        );
        assert2::assert!(
            stats
                == super::super::QuerySampleStats {
                    total_queryable_samples: total,
                    peak_samples: peak,
                    per_step: BTreeMap::new(),
                },
            "{query}"
        );
    }
}
