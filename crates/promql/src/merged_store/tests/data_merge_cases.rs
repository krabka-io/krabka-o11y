use super::*;

mod duplicate_samples_keep_hot_values {
use assert2::check;
use krabka_metrics::{NativeHistogram, ResetHint};

use super::*;
use crate::extension::STALE_NAN_BITS;

const STALE_NAN: f64 = f64::from_bits(STALE_NAN_BITS);

struct DuplicateFloatCase {
    cold_value: f64,
    hot_value: f64,
    expected: Option<f64>,
}

#[tokio::test]
pub(crate) async fn duplicate_samples_keep_hot_values() {
    let tenant = tenant_id("t");
    let series = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    for case in [
        DuplicateFloatCase {
            cold_value: 1.0,
            hot_value: 7.0,
            expected: Some(7.0),
        },
        DuplicateFloatCase {
            cold_value: 7.0,
            hot_value: 1.0,
            expected: Some(1.0),
        },
        DuplicateFloatCase {
            cold_value: 1.0,
            hot_value: STALE_NAN,
            expected: None,
        },
        DuplicateFloatCase {
            cold_value: STALE_NAN,
            hot_value: 7.0,
            expected: Some(7.0),
        },
    ] {
        let mut cold = InMemoryMetricStore::new();
        let mut hot = InMemoryMetricStore::new();
        cold.push_float("t", series.clone(), 10_000, case.cold_value);
        hot.push_float("t", series.clone(), 10_000, case.hot_value);
        // A duplicate within the same source must also appear only once.
        hot.push_float("t", series.clone(), 10_000, case.hot_value);
        let engine = PromqlEngine::new(
            Arc::new(MergedMetricStore::new(cold, hot)),
            EngineOpts::default(),
        );
        let samples = case
            .expected
            .map(|value| InstantSample {
                labels: series.clone(),
                ts_ms: 10_000,
                value: SampleValue::Float(value),
                drop_name: false,
            })
            .into_iter()
            .collect();
        check!(
            engine.query_instant(&tenant, "up", 10_000).await.unwrap()
                == QueryResult::InstantVector(samples)
        );
    }

    let series = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "latency"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    let mut histogram = NativeHistogram {
        schema: 0,
        is_float: true,
        reset_hint: ResetHint::No,
        zero_threshold: 0.0,
        zero_count: 2.0,
        count: 2.0,
        sum: 2.0,
        positive_spans: Vec::new(),
        positive_counts: Vec::new(),
        negative_spans: Vec::new(),
        negative_counts: Vec::new(),
        custom_values: None,
        start_timestamp_ms: None,
    };
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    cold.push_histogram("t", series.clone(), 10_000, histogram.clone());
    histogram.zero_count = 7.0;
    histogram.count = 7.0;
    histogram.sum = 7.0;
    hot.push_histogram("t", series.clone(), 10_000, histogram.clone());
    let engine = PromqlEngine::new(
        Arc::new(MergedMetricStore::new(cold, hot)),
        EngineOpts::default(),
    );
    check!(
        engine
            .query_instant(&tenant, "latency", 10_000)
            .await
            .unwrap()
            == QueryResult::InstantVector(vec![InstantSample {
                labels: series,
                ts_ms: 10_000,
                value: SampleValue::Histogram(histogram),
                drop_name: false,
            }])
    );
}

}

mod exemplars_merges_cold_and_hot_records {
use super::*;

#[tokio::test]
pub(crate) async fn exemplars_merges_cold_and_hot_records() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    let series = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "request_latency"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    cold.push_exemplar(
        "tenant-a",
        series.clone(),
        labels(&[crate::test_support::TestLabelPair::new("trace_id", "cold")]),
        10_000,
        1.0,
    );
    hot.push_exemplar(
        "tenant-a",
        series.clone(),
        labels(&[crate::test_support::TestLabelPair::new("trace_id", "hot")]),
        20_000,
        2.0,
    );

    let store = MergedMetricStore::new(cold, hot);
    let exemplars = store
        .exemplars("tenant-a", &[], 0, 30_000)
        .await
        .unwrap()
        .exemplars;

    assert2::assert!(
        exemplars
            == vec![
                ExemplarRecord {
                    series_labels: series.clone(),
                    labels: labels(&[crate::test_support::TestLabelPair::new("trace_id", "cold")]),
                    ts_ms: 10_000,
                    value: 1.0,
                },
                ExemplarRecord {
                    series_labels: series,
                    labels: labels(&[crate::test_support::TestLabelPair::new("trace_id", "hot")]),
                    ts_ms: 20_000,
                    value: 2.0,
                },
            ]
    );
}

}

mod histograms_in_either_store_are_found {
use assert2::check;
use krabka_blockstore::MatchOp;
use krabka_metrics::{NativeHistogram, ResetHint};

use super::*;
use crate::PromqlMatcher as LabelMatcher;

struct HistogramAvailabilityCase {
    cold_has_histogram: bool,
    hot_has_histogram: bool,
    expected: bool,
}

fn histogram(count: f64) -> NativeHistogram {
    NativeHistogram {
        schema: 0,
        is_float: true,
        reset_hint: ResetHint::No,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count,
        sum: count,
        positive_spans: Vec::new(),
        positive_counts: Vec::new(),
        negative_spans: Vec::new(),
        negative_counts: Vec::new(),
        custom_values: None,
        start_timestamp_ms: None,
    }
}

/// A store with one float series, and with one histogram series when
/// `with_histogram` is set.
fn store(with_histogram: bool) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "t",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "up"),
            crate::test_support::TestLabelPair::new("job", "api"),
        ]),
        60_000,
        1.0,
    );
    if with_histogram {
        for ts_ms in [60_000, 120_000] {
            store.push_histogram(
                "t",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "latency"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                ]),
                ts_ms,
                histogram(2.0),
            );
        }
    }
    store
}

#[tokio::test]
pub(crate) async fn histograms_in_either_store_are_found() {
    let matchers = [LabelMatcher {
        name: "job".to_string(),
        op: MatchOp::Eq,
        value: "api".to_string().into(),
    }];
    // A scan merges the histogram tables of both stores, so the merged store
    // can say "no histograms" only when both stores say it.
    for case in [
        HistogramAvailabilityCase {
            cold_has_histogram: false,
            hot_has_histogram: false,
            expected: false,
        },
        HistogramAvailabilityCase {
            cold_has_histogram: true,
            hot_has_histogram: false,
            expected: true,
        },
        HistogramAvailabilityCase {
            cold_has_histogram: false,
            hot_has_histogram: true,
            expected: true,
        },
        HistogramAvailabilityCase {
            cold_has_histogram: true,
            hot_has_histogram: true,
            expected: true,
        },
    ] {
        let merged = MergedMetricStore::new(
            store(case.cold_has_histogram),
            store(case.hot_has_histogram),
        );
        check!(
            merged
                .may_have_histograms("t", &matchers, 0, 200_000)
                .await
                .unwrap()
                == case.expected,
            "cold={} hot={}",
            case.cold_has_histogram,
            case.hot_has_histogram
        );
    }

    // The hot store holds a histogram that the float-only cold store does
    // not. The query still counts it.
    let engine = PromqlEngine::new(
        Arc::new(MergedMetricStore::new(store(false), store(true))),
        EngineOpts::default(),
    );
    let result = engine
        .query_instant(&tenant_id("t"), "count_over_time(latency[2m])", 120_000)
        .await
        .unwrap();
    check!(
        result
            == QueryResult::InstantVector(vec![InstantSample {
                labels: labels(&[crate::test_support::TestLabelPair::new("job", "api")]),
                ts_ms: 120_000,
                value: SampleValue::Float(2.0),
                drop_name: false,
            }])
    );
}

}

mod instant_query_uses_hot_sample_newer_than_compacted_sample {
use super::*;

#[tokio::test]
pub(crate) async fn instant_query_uses_hot_sample_newer_than_compacted_sample() {
    let labels = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    let store = merged_store_with_samples(&[up_sample(10_000, 1.0)], &[up_sample(20_000, 2.0)]);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(&tenant_id("tenant-a"), "up", 20_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected instant vector");
    };
    assert2::assert!(
        samples
            == vec![InstantSample {
                labels,
                ts_ms: 20_000,
                value: SampleValue::Float(2.0),
                drop_name: false,
            }]
    );
}

}

mod range_query_counts_sample_present_in_both_stores_once {
use super::*;

#[tokio::test]
pub(crate) async fn range_query_counts_sample_present_in_both_stores_once() {
    // The same (fingerprint, timestamp) sample lives in both cold and hot —
    // the steady state, since hot retention is time-based and independent of
    // compaction. Without (fp, ts) dedup the merged scan double-counts it.
    // Hot re-reports the 20s sample (still within hot retention) and adds 30s.
    let store = merged_store_with_samples(
        &[up_sample(10_000, 1.0), up_sample(20_000, 1.0)],
        &[up_sample(20_000, 1.0), up_sample(30_000, 1.0)],
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let result = engine
        .query_instant(&tenant_id("tenant-a"), "count_over_time(up[1m])", 30_000)
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected instant vector");
    };
    assert2::assert!(samples.len() == 1);
    // Three distinct timestamps (10s, 20s, 30s); the duplicated 20s sample
    // must be counted once, not twice.
    assert2::assert!(samples[0].value == SampleValue::Float(3.0));

    // A windowed sum must likewise see each timestamp once.
    let result = engine
        .query_instant(&tenant_id("tenant-a"), "sum_over_time(up[1m])", 30_000)
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected instant vector");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].value == SampleValue::Float(3.0));
}

}
