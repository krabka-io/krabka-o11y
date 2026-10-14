use super::*;

#[derive(bon::Builder)]
struct ClassicBucket {
    upper_bound: &'static str,
    cumulative_count: f64,
}

fn mixed_metric_labels(metric: &str, instance: &str) -> Labels {
    labels(&[
        crate::test_support::TestLabelPair::new("__name__", metric),
        crate::test_support::TestLabelPair::new("job", "api"),
        crate::test_support::TestLabelPair::new("instance", instance),
    ])
}

pub(crate) fn mixed_histogram_store() -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "series"),
            crate::test_support::TestLabelPair::new("host", "a"),
        ]),
        0,
        native_histogram(4.0, 5.0),
    );
    for bucket in [
        ClassicBucket::builder()
            .upper_bound("0.1")
            .cumulative_count(2.0)
            .build(),
        ClassicBucket::builder()
            .upper_bound("1")
            .cumulative_count(3.0)
            .build(),
        ClassicBucket::builder()
            .upper_bound("+Inf")
            .cumulative_count(9.0)
            .build(),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "series"),
                crate::test_support::TestLabelPair::new("host", "a"),
                crate::test_support::TestLabelPair::new("le", bucket.upper_bound),
            ]),
            0,
            bucket.cumulative_count,
        );
    }
    store
}

/// A store holding two `mixed_metric{job="api"}` series at 10s: instance
/// `float` with the float 4 and instance `hist` with a native histogram of
/// count 4 and sum 10.
pub(crate) fn mixed_api_group_store() -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        mixed_metric_labels("mixed_metric", "float"),
        10_000,
        4.0,
    );
    store.push_histogram(
        "tenant-a",
        mixed_metric_labels("mixed_metric", "hist"),
        10_000,
        native_histogram(4.0, 10.0),
    );
    store
}

#[derive(bon::Builder)]
pub(crate) struct MixedMetricFloatSample {
    pub(crate) instance: &'static str,
    pub(crate) value: f64,
}

pub(crate) fn mixed_metric_store(
    metric: &'static str,
    float_samples: &[MixedMetricFloatSample],
) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    for sample in float_samples {
        store.push_float(
            "tenant-a",
            mixed_metric_labels(metric, sample.instance),
            10_000,
            sample.value,
        );
    }
    store.push_histogram(
        "tenant-a",
        mixed_metric_labels(metric, "hist"),
        10_000,
        native_histogram(4.0, 10.0),
    );
    store
}
