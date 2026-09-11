use super::*;

// Two `up` series in one tenant, at timestamp zero. Every series-cap test needs
// a tenant whose selected-series count is above a cap of one.
pub(crate) fn two_series_store() -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    for job in ["api", "worker"] {
        let mut labels = Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", job);
        store.push_float("tenant-a", labels, 0, 1.0);
    }
    store
}
