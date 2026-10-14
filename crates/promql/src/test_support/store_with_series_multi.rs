use super::*;

#[derive(bon::Builder)]
pub(crate) struct TestSeriesValue {
    selector: &'static str,
    value: f64,
}

pub(crate) fn store_with_series_multi(series: &[TestSeriesValue]) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    for sample in series {
        store.push_float(TENANT, metric_to_labels(sample.selector), 0, sample.value);
    }
    store
}
