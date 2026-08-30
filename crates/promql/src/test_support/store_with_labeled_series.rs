use super::*;

pub(crate) fn store_with_labeled_series(
    name: &str,
    labels: &[(&str, &str)],
    value: f64,
) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    let mut label_set = Labels::new();
    label_set.insert("__name__", name);
    for (key, value) in labels {
        label_set.insert(*key, *value);
    }
    store.push_float(TENANT, label_set, 0, value);
    store
}
