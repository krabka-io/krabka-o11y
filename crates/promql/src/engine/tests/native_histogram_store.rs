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
