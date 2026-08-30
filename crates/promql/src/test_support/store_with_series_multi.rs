use super::*;

pub(crate) fn store_with_series_multi(series: &[(&str, f64)]) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    for (selector, value) in series {
        store.push_float(TENANT, metric_to_labels(selector), 0, *value);
    }
    store
}
