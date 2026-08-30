use super::*;

/// `increase` over native histograms recompacts the result's spans: buckets
/// that did not move are dropped from both ends, and the gap between the two
/// input spans splits what remains into two runs. Every other histogram-rate
/// test uses a single dense span where the compaction has nothing to do --
/// no edge to strip, no gap to split on, and no second span offset to add in.
#[tokio::test]
pub(crate) async fn increase_recompacts_native_histogram_spans_around_gaps() {
    let histogram = |counts: Vec<f64>| NativeHistogram {
        schema: 0,
        is_float: true,
        reset_hint: ResetHint::No,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count: counts.iter().sum(),
        sum: 0.0,
        positive_spans: vec![
            BucketSpan {
                offset: 0,
                length: 3,
            },
            BucketSpan {
                offset: 2,
                length: 3,
            },
            // A third run, so the second emitted span has a non-zero
            // `previous_span_end` to subtract -- at the first one it is still
            // zero, where subtracting and adding it agree.
            BucketSpan {
                offset: 2,
                length: 3,
            },
        ],
        positive_counts: counts,
        negative_spans: vec![],
        negative_counts: vec![],
        custom_values: None,
        start_timestamp_ms: None,
    };

    let mut store = InMemoryMetricStore::new();
    // The outermost buckets hold still, as does one in the middle of a run.
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "h")]),
        10_000,
        histogram(vec![1.0; 9]),
    );
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "h")]),
        20_000,
        histogram(vec![1.0, 3.0, 4.0, 5.0, 6.0, 1.0, 7.0, 8.0, 1.0]),
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "increase(h[5m])", 20_000)
        .await
        .expect("a histogram increase");
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected a vector");
    };
    assert2::assert!(samples.len() == 1);
    let SampleValue::Histogram(histogram) = &samples[0].value else {
        panic!("expected a histogram sample");
    };

    // The input spans cover 0..=2, 5..=7 and 10..=12. Buckets 0 and 12 hold
    // still and are trimmed; the gaps at 3..=4 and 8..=9 split the rest into
    // three runs. Bucket 7 holds still too but sits inside a run, so it stays.
    check!(
        histogram.positive_spans
            == vec![
                BucketSpan {
                    offset: 1,
                    length: 2
                },
                BucketSpan {
                    offset: 2,
                    length: 3
                },
                BucketSpan {
                    offset: 2,
                    length: 2
                },
            ]
    );
    check!(histogram.positive_counts.len() == 7);
}
