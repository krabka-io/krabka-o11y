use super::*;

pub(crate) fn mixed_histogram_store() -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "series"), ("host", "a")]),
        0,
        native_histogram(4.0, 5.0),
    );
    for (le, value) in [("0.1", 2.0), ("1", 3.0), ("+Inf", 9.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "series"), ("host", "a"), ("le", le)]),
            0,
            value,
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
        labels(&[
            ("__name__", "mixed_metric"),
            ("job", "api"),
            ("instance", "float"),
        ]),
        10_000,
        4.0,
    );
    store.push_histogram(
        "tenant-a",
        labels(&[
            ("__name__", "mixed_metric"),
            ("job", "api"),
            ("instance", "hist"),
        ]),
        10_000,
        native_histogram(4.0, 10.0),
    );
    store
}
