use super::*;

#[tokio::test]
pub(crate) async fn scan_filters_histograms_by_matcher_tenant_and_time() {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "t",
        lbls(&[("__name__", "latency_seconds"), ("job", "api")]),
        1_000,
        native_histogram(),
    );
    store.push_histogram(
        "t",
        lbls(&[("__name__", "latency_seconds"), ("job", "api")]),
        5_000,
        native_histogram(),
    );
    store.push_histogram(
        "t",
        lbls(&[("__name__", "latency_seconds"), ("job", "worker")]),
        1_000,
        native_histogram(),
    );
    store.push_histogram(
        "other",
        lbls(&[("__name__", "latency_seconds"), ("job", "api")]),
        1_000,
        native_histogram(),
    );

    let matchers = [
        LabelMatcher::new("__name__", MatchOp::Eq, "latency_seconds"),
        LabelMatcher::new("job", MatchOp::Eq, "api"),
    ];
    let result = store.scan("t", &matchers, 0, 1_500).await.unwrap();
    assert2::assert!(result.float_table.is_none());
    let table = result.histogram_table.clone().unwrap();
    assert2::assert!(count_rows(&result, &table).await == 1);
}
