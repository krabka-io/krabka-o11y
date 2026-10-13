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
