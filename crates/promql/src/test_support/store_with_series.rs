use super::*;

pub(crate) fn store_with_series(name: &str, samples: &[(i64, f64)]) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    let mut labels = Labels::new();
    labels.insert("__name__", name);
    for (ts_ms, value) in samples {
        store.push_float(TENANT, labels.clone(), *ts_ms, *value);
    }
    store
}
