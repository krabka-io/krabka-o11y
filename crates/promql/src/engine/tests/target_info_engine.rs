use super::*;

/// An engine over `http_requests_total{job="api",instance="a"}` = 7 and its
/// `target_info` series carrying `region="east"` and `cluster="prod"`, at 10s.
pub(crate) fn target_info_engine() -> PromqlEngine<InMemoryMetricStore> {
    info_metrics_engine(vec![info_series_labels(
        "target_info",
        &[("region", "east"), ("cluster", "prod")],
    )])
}

/// An engine over `http_requests_total{job="api",instance="a"}` = 7 and each
/// of `info_series` = 1, all at 10s.
pub(crate) fn info_metrics_engine(info_series: Vec<Labels>) -> PromqlEngine<InMemoryMetricStore> {
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", request_metric_labels(&[]), 10_000, 7.0);
    for series in info_series {
        store.push_float("tenant-a", series, 10_000, 1.0);
    }
    PromqlEngine::new(Arc::new(store), EngineOpts::default())
}

/// Builds the shared identifying labels used by the info-function fixtures.
pub(crate) fn info_series_labels(metric_name: &str, data_labels: &[(&str, &str)]) -> Labels {
    metric_labels(metric_name, data_labels)
}

/// Builds the request metric labels, optionally adding data labels to it.
pub(crate) fn request_metric_labels(data_labels: &[(&str, &str)]) -> Labels {
    metric_labels("http_requests_total", data_labels)
}

fn metric_labels(metric_name: &str, additional_labels: &[(&str, &str)]) -> Labels {
    let mut pairs = vec![
        crate::test_support::TestLabelPair::new("__name__", metric_name),
        crate::test_support::TestLabelPair::new("job", "api"),
        crate::test_support::TestLabelPair::new("instance", "a"),
    ];
    pairs.extend(
        additional_labels
            .iter()
            .map(|(name, value)| crate::test_support::TestLabelPair::new(*name, *value)),
    );
    labels(&pairs)
}
