use super::*;

pub(crate) fn store_with_float_and_hist_series() -> (InMemoryMetricStore, Labels) {
    let mut store = InMemoryMetricStore::new();
    let up_api = lbls(&[("__name__", "up"), ("job", "api")]);
    let up_worker = lbls(&[("__name__", "up"), ("job", "worker")]);
    let latency = lbls(&[("__name__", "latency_seconds"), ("job", "api")]);
    store.push_float("tenant-a", up_api.clone(), 1_000, 1.0);
    store.push_float("tenant-a", up_worker, 2_000, 2.0);
    store.push_histogram("tenant-a", latency, 3_000, native_histogram());
    (store, up_api)
}
