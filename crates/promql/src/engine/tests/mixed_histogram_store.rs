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
