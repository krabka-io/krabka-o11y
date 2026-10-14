use super::*;

#[derive(bon::Builder)]
pub(crate) struct FloatAndHistogramStoreFixture {
    pub(crate) store: InMemoryMetricStore,
    pub(crate) up_api: Labels,
}

pub(crate) fn store_with_float_and_hist_series() -> FloatAndHistogramStoreFixture {
    let mut store = InMemoryMetricStore::new();
    let up_api = lbls(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    let up_worker = lbls(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "worker"),
    ]);
    let latency = lbls(&[
        crate::test_support::TestLabelPair::new("__name__", "latency_seconds"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    store.push_float("tenant-a", up_api.clone(), 1_000, 1.0);
    store.push_float("tenant-a", up_worker, 2_000, 2.0);
    store.push_histogram("tenant-a", latency, 3_000, count_two_sum_three_histogram());
    FloatAndHistogramStoreFixture::builder()
        .store(store)
        .up_api(up_api)
        .build()
}
