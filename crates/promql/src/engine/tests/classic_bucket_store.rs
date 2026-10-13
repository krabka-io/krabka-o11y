use super::*;

#[derive(bon::Builder)]
struct ClassicBucket {
    upper_bound: &'static str,
    cumulative_count: f64,
}

/// A store holding classic `http_request_duration_seconds_bucket{job="api"}`
/// buckets at 10s: `le` 0.1 = 0, 0.2 = 1, 0.4 = 3 and `+Inf` = 3.
pub(crate) fn classic_bucket_store() -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    for bucket in [
        ClassicBucket::builder()
            .upper_bound("0.1")
            .cumulative_count(0.0)
            .build(),
        ClassicBucket::builder()
            .upper_bound("0.2")
            .cumulative_count(1.0)
            .build(),
        ClassicBucket::builder()
            .upper_bound("0.4")
            .cumulative_count(3.0)
            .build(),
        ClassicBucket::builder()
            .upper_bound("+Inf")
            .cumulative_count(3.0)
            .build(),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new(
                    "__name__",
                    "http_request_duration_seconds_bucket",
                ),
                crate::test_support::TestLabelPair::new("job", "api"),
                crate::test_support::TestLabelPair::new("le", bucket.upper_bound),
            ]),
            10_000,
            bucket.cumulative_count,
        );
    }
    store
}
