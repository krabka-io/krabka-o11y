use super::*;

/// An engine over `http_requests_total{job="api",instance="a"}` = 7 and its
/// `target_info` series carrying `region="east"` and `cluster="prod"`, at 10s.
pub(crate) fn target_info_engine() -> PromqlEngine<InMemoryMetricStore> {
    info_metrics_engine(vec![labels(&[
        ("__name__", "target_info"),
        ("job", "api"),
        ("instance", "a"),
        ("region", "east"),
        ("cluster", "prod"),
    ])])
}

/// An engine over `http_requests_total{job="api",instance="a"}` = 7 and each
/// of `info_series` = 1, all at 10s.
pub(crate) fn info_metrics_engine(info_series: Vec<Labels>) -> PromqlEngine<InMemoryMetricStore> {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "http_requests_total"),
            ("job", "api"),
            ("instance", "a"),
        ]),
        10_000,
        7.0,
    );
    for series in info_series {
        store.push_float("tenant-a", series, 10_000, 1.0);
    }
    PromqlEngine::new(Arc::new(store), EngineOpts::default())
}
