use super::*;

pub(crate) fn native_histogram_store() -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
        10_000,
        native_histogram(4.0, 10.0),
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
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
        10_000,
        histogram,
    );
    store
}

/// A store holding `request_duration_seconds{job="api"}` native histograms of
/// count 4 at 60s (sum 10) and 120s (sum 20).
pub(crate) fn histogram_sum_series_store() -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, sum) in [(60_000, 10.0), (120_000, 20.0)] {
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
            ts_ms,
            native_histogram(4.0, sum),
        );
    }
    store
}

/// The native histograms of instances `a` and `b`.
pub(crate) struct InstanceHistograms {
    pub(crate) a: NativeHistogram,
    pub(crate) b: NativeHistogram,
}

/// A store holding `request_duration_seconds{job="api"}` at 10s, with one
/// native histogram per instance.
pub(crate) fn instance_histogram_store(histograms: InstanceHistograms) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    for (instance, histogram) in [("a", histograms.a), ("b", histograms.b)] {
        store.push_histogram(
            "tenant-a",
            labels(&[
                ("__name__", "request_duration_seconds"),
                ("job", "api"),
                ("instance", instance),
            ]),
            10_000,
            histogram,
        );
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
