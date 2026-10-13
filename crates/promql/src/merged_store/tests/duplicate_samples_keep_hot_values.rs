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
