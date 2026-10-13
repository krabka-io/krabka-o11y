use super::*;

struct HistogramSample {
    timestamp_ms: i64,
    sum: f64,
}

struct NamedHistogram {
    instance: &'static str,
    histogram: NativeHistogram,
}

pub(crate) fn native_histogram_store() -> InMemoryMetricStore {
    store_with_request_duration_histogram(native_histogram(4.0, 10.0))
}

pub(crate) fn single_bucket_instance_histogram(
    count: f64,
    sum: f64,
    schema: i8,
    offset: i32,
    bucket_count: f64,
) -> NativeHistogram {
    let mut histogram = native_histogram(count, sum);
    histogram.schema = schema;
    histogram.positive_spans = vec![BucketSpan { offset, length: 1 }];
    histogram.positive_counts = vec![bucket_count];
    histogram
}

pub(crate) fn store_with_request_duration_histogram(
    histogram: NativeHistogram,
) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "request_duration_seconds"),
            crate::test_support::TestLabelPair::new("job", "api"),
        ]),
        10_000,
        histogram,
    );
    store
}

/// A store holding `request_duration_seconds{job="api"}` at 10s: a native
/// histogram of count 4 and the given `sum`, with buckets `(0.5, 1]` = 1 and
/// `(1, 2]` = 3.
pub(crate) fn two_bucket_histogram_store(sum: f64) -> InMemoryMetricStore {
    let mut histogram = native_histogram(4.0, sum);
    histogram.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 2,
    }];
    histogram.positive_counts = vec![1.0, 3.0];
    store_with_request_duration_histogram(histogram)
}

/// A store holding `request_duration_seconds{job="api"}` native histograms of
/// count 4 at 60s (sum 10) and 120s (sum 20).
pub(crate) fn histogram_sum_series_store() -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    for sample in [
        HistogramSample {
            timestamp_ms: 60_000,
            sum: 10.0,
        },
        HistogramSample {
            timestamp_ms: 120_000,
            sum: 20.0,
        },
    ] {
        store.push_histogram(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "request_duration_seconds"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]),
            sample.timestamp_ms,
            native_histogram(4.0, sample.sum),
        );
    }
    store
}

/// The native histograms of instances `a` and `b`.
pub(crate) struct InstanceHistograms {
    pub(crate) a: NativeHistogram,
    pub(crate) b: NativeHistogram,
}

pub(crate) fn push_request_duration_instance_histogram(
    store: &mut InMemoryMetricStore,
    instance: &str,
    histogram: NativeHistogram,
) {
    store.push_histogram(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "request_duration_seconds"),
            crate::test_support::TestLabelPair::new("job", "api"),
            crate::test_support::TestLabelPair::new("instance", instance),
        ]),
        10_000,
        histogram,
    );
}

/// A store holding `request_duration_seconds{job="api"}` at 10s, with one
/// native histogram per instance.
pub(crate) fn instance_histogram_store(histograms: InstanceHistograms) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    for sample in [
        NamedHistogram {
            instance: "a",
            histogram: histograms.a,
        },
        NamedHistogram {
            instance: "b",
            histogram: histograms.b,
        },
    ] {
        push_request_duration_instance_histogram(&mut store, sample.instance, sample.histogram);
    }
    store
}

/// Checks `query` at 120s over [`histogram_sum_series_store`]: one unnamed
/// `job="api"` float sample approximately `expected`.
pub(crate) async fn assert_histogram_series_reduction(query: &str, expected: f64) {
    let engine = PromqlEngine::new(
        Arc::new(histogram_sum_series_store()),
        EngineOpts::default(),
    );
    let samples = instant_vector(&engine, query, 120_000).await;
    check!(samples.len() == 1, "{query}");
    check!(samples[0].labels.get("__name__").is_none(), "{query}");
    check!(samples[0].labels.get("job") == Some("api"), "{query}");
    check!(
        approx_eq(float_value(&samples[0].value), expected),
        "{query}"
    );
}

/// An engine over a store holding `h` at 10s: a float native histogram of
/// count 12 and the given `sum`, with buckets -4..-2 = 3 and -2..-1 = 1, a
/// zero bucket of threshold 0.5 holding 2, then 1..2 = 4 and 2..4 = 2.
pub(crate) fn signed_bucket_histogram_engine(sum: f64) -> PromqlEngine<InMemoryMetricStore> {
    let mut histogram = native_histogram(12.0, sum);
    histogram.zero_threshold = 0.5;
    histogram.zero_count = 2.0;
    histogram.positive_spans = vec![BucketSpan {
        offset: 1,
        length: 2,
    }];
    histogram.positive_counts = vec![4.0, 2.0];
    histogram.negative_spans = vec![BucketSpan {
        offset: 1,
        length: 2,
    }];
    histogram.negative_counts = vec![3.0, 1.0];
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[crate::test_support::TestLabelPair::new("__name__", "h")]),
        10_000,
        histogram,
    );
    PromqlEngine::new(Arc::new(store), EngineOpts::default())
}
