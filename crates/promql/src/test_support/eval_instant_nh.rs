use super::*;

pub(crate) fn eval_instant_nh(name: &str, histogram: &NativeHistogram) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    let mut labels = Labels::new();
    labels.insert("__name__", name);
    store.push_histogram(TENANT, labels, 0, histogram.clone());
    store
}
