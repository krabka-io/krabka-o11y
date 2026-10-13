use super::*;

#[derive(bon::Builder)]
pub(crate) struct TestSample {
    timestamp_ms: i64,
    value: f64,
}

pub(crate) fn store_with_series(name: &str, samples: &[TestSample]) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    let mut labels = Labels::new();
    labels.insert("__name__", name);
    for sample in samples {
        store.push_float(TENANT, labels.clone(), sample.timestamp_ms, sample.value);
    }
    store
}
