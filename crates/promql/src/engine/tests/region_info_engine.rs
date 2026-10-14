use super::*;

/// One `http_requests_total{job, instance}` series and its value at 10s.
#[derive(Clone, Copy)]
pub(crate) struct InstanceRequests<'a> {
    pub(crate) job: &'a str,
    pub(crate) instance: &'a str,
    pub(crate) total: f64,
}

/// An engine over each of `requests` and
/// `target_info{job="api",region="east"}` = 10, all at 10s.
pub(crate) fn region_info_engine(
    requests: &[InstanceRequests<'_>],
) -> PromqlEngine<InMemoryMetricStore> {
    let mut store = InMemoryMetricStore::new();
    for series in requests {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
                crate::test_support::TestLabelPair::new("job", series.job),
                crate::test_support::TestLabelPair::new("instance", series.instance),
            ]),
            10_000,
            series.total,
        );
    }
    store.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "target_info"),
            crate::test_support::TestLabelPair::new("job", "api"),
            crate::test_support::TestLabelPair::new("region", "east"),
        ]),
        10_000,
        10.0,
    );
    PromqlEngine::new(Arc::new(store), EngineOpts::default())
}
