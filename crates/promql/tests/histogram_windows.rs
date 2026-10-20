use std::sync::Arc;

use krabka_blockstore::{Labels, TenantId};
use krabka_metrics::{BucketSpan, NativeHistogram, ResetHint};
use krabka_promql::{Annotations, EngineOpts, InMemoryMetricStore, PromqlEngine, QueryResult};
use krabka_units::prelude::*;

fn histogram(step: u32, custom: bool) -> NativeHistogram {
    let count = f64::from(step % 4 + 1);
    NativeHistogram {
        schema: if custom { -53 } else { i8::from(step % 2 == 0) },
        is_float: true,
        reset_hint: match step % 4 {
            0 => ResetHint::Unknown,
            1 => ResetHint::No,
            2 => ResetHint::Yes,
            _ => ResetHint::Gauge,
        },
        zero_threshold: 0.0,
        zero_count: if custom { 0.0 } else { count },
        count: count * 4.0,
        sum: count * 8.0,
        positive_spans: vec![BucketSpan {
            offset: 0,
            length: 2,
        }],
        positive_counts: vec![count, count * 2.0],
        negative_spans: vec![],
        negative_counts: vec![],
        custom_values: custom.then(|| if step % 2 == 0 { vec![1.0] } else { vec![2.0] }),
        start_timestamp_ms: Some(i64::from(step / 4) * 60_000),
    }
}

fn store() -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    for instance in ["exponential", "custom", "mixed", "sparse"] {
        let mut labels = Labels::new();
        labels.insert("__name__", "duration_seconds");
        labels.insert("instance", instance);
        // Reverse input order and replayed timestamps exercise stable duplicate resolution.
        for step in (0..=12).rev() {
            let ts = i64::from(step) * 15_000;
            if instance == "sparse" && step % 4 != 0 {
                continue;
            }
            if instance == "mixed" && step % 3 == 0 {
                store.push_float("test", labels.clone(), ts, f64::from(step));
            } else {
                store.push_histogram(
                    "test",
                    labels.clone(),
                    ts,
                    histogram(step, instance == "custom"),
                );
                if step == 4 {
                    store.push_histogram(
                        "test",
                        labels.clone(),
                        ts,
                        histogram(step + 1, instance == "custom"),
                    );
                }
            }
        }
        // Histograms win same-timestamp collisions in matrix selectors.
        store.push_float("test", labels, 75_000, 99.0);
    }
    store
}

#[tokio::test]
async fn cached_histogram_windows_match_independent_instant_queries() {
    let engine = PromqlEngine::new(Arc::new(store()), EngineOpts::default());
    let tenant = TenantId::new("test").unwrap();
    for function in [
        "rate",
        "increase",
        "delta",
        "irate",
        "idelta",
        "changes",
        "resets",
        "sum_over_time",
        "avg_over_time",
        "count_over_time",
        "last_over_time",
        "present_over_time",
    ] {
        for selector in ["duration_seconds[1m]", "duration_seconds[1m] offset 15s"] {
            let query = format!("{function}({selector})");
            let actual = engine
                .query_range_with_annotations(&tenant, &query, 60_000, 180_000, millis(15_000))
                .await
                .unwrap();
            let QueryResult::RangeMatrix(matrix) = actual.result else {
                panic!("expected matrix")
            };
            let mut expected_annotations = Annotations::new();
            let mut count = 0;
            for time_ms in (60_000..=180_000).step_by(15_000) {
                let instant = engine
                    .query_instant_with_annotations(&tenant, &query, time_ms)
                    .await
                    .unwrap();
                expected_annotations.extend(&instant.annotations);
                let QueryResult::InstantVector(samples) = instant.result else {
                    panic!("expected vector")
                };
                count += samples.len();
                for sample in samples {
                    let series = matrix
                        .iter()
                        .find(|series| series.labels == sample.labels)
                        .expect("series from instant query");
                    let actual_sample = series
                        .samples
                        .iter()
                        .find(|sample| sample.timestamp_ms == time_ms)
                        .expect("step from instant query");
                    assert2::assert!(actual_sample.value == sample.value, "{query} at {time_ms}");
                }
            }
            assert2::assert!(
                matrix
                    .iter()
                    .map(|series| series.samples.len())
                    .sum::<usize>()
                    == count
            );
            let mut actual_warnings = actual.annotations.warnings;
            let mut actual_infos = actual.annotations.infos;
            actual_warnings.sort();
            actual_infos.sort();
            expected_annotations.warnings.sort();
            expected_annotations.infos.sort();
            assert2::assert!(actual_warnings == expected_annotations.warnings, "{query}");
            assert2::assert!(actual_infos == expected_annotations.infos, "{query}");
        }
    }
}
