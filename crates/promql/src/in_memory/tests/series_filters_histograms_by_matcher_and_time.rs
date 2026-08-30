use super::*;

#[tokio::test]
pub(crate) async fn series_filters_histograms_by_matcher_and_time() {
    let mut store = InMemoryMetricStore::new();
    let api = lbls(&[("__name__", "latency_seconds"), ("job", "api")]);
    let worker = lbls(&[("__name__", "latency_seconds"), ("job", "worker")]);
    store.push_histogram("t", api.clone(), 1_000, native_histogram());
    store.push_histogram("t", api.clone(), 5_000, native_histogram());
    store.push_histogram("t", worker, 1_000, native_histogram());

    let matchers = [
        LabelMatcher::new("__name__", MatchOp::Eq, "latency_seconds"),
        LabelMatcher::new("job", MatchOp::Eq, "api"),
    ];
    let series = store.series("t", &matchers, 0, 1_500).await.unwrap();
    assert2::assert!(series == vec![api]);
}
