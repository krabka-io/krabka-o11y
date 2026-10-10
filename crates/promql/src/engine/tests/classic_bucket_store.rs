use super::*;

/// A store holding classic `http_request_duration_seconds_bucket{job="api"}`
/// buckets at 10s: `le` 0.1 = 0, 0.2 = 1, 0.4 = 3 and `+Inf` = 3.
pub(crate) fn classic_bucket_store() -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    for (le, value) in [("0.1", 0.0), ("0.2", 1.0), ("0.4", 3.0), ("+Inf", 3.0)] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "http_request_duration_seconds_bucket"),
                ("job", "api"),
                ("le", le),
            ]),
            10_000,
            value,
        );
    }
    store
}
