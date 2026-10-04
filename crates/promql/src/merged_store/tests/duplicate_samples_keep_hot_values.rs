use assert2::check;
use krabka_metrics::{NativeHistogram, ResetHint};

use super::*;
use crate::extension::STALE_NAN_BITS;

const STALE_NAN: f64 = f64::from_bits(STALE_NAN_BITS);

#[tokio::test]
pub(crate) async fn duplicate_samples_keep_hot_values() {
    let tenant = tenant_id("t");
    let series = labels(&[("__name__", "up"), ("job", "api")]);
    for (cold_value, hot_value, expected) in [
        (1.0, 7.0, Some(7.0)),
        (7.0, 1.0, Some(1.0)),
        (1.0, STALE_NAN, None),
        (STALE_NAN, 7.0, Some(7.0)),
    ] {
        let mut cold = InMemoryMetricStore::new();
        let mut hot = InMemoryMetricStore::new();
        cold.push_float("t", series.clone(), 10_000, cold_value);
        hot.push_float("t", series.clone(), 10_000, hot_value);
        // A duplicate within the same source must also appear only once.
        hot.push_float("t", series.clone(), 10_000, hot_value);
        let engine = PromqlEngine::new(
            Arc::new(MergedMetricStore::new(cold, hot)),
            EngineOpts::default(),
        );
        let samples = expected
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

    let series = labels(&[("__name__", "latency"), ("job", "api")]);
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
