use super::*;

pub(crate) fn store_with_labeled_series(
    name: &str,
    labels: &[TestLabelPair<'_>],
    value: f64,
) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    let mut label_set = Labels::new();
    label_set.insert("__name__", name);
    for pair in labels {
        label_set.insert(pair.name, pair.value);
    }
    store.push_float(TENANT, label_set, 0, value);
    store
}
