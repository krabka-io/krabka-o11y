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

pub(crate) fn up_api_store(tenant: &str, samples: &[(i64, f64)]) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    let mut labels = Labels::new();
    labels.insert("__name__", "up");
    labels.insert("job", "api");
    for &(timestamp_ms, value) in samples {
        store.push_float(tenant, labels.clone(), timestamp_ms, value);
    }
    store
}
