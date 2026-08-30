use super::*;

#[tokio::test]
pub(crate) async fn scan_with_no_match_returns_none_tables() {
    let mut store = InMemoryMetricStore::new();
    store.push_float("t", lbls(&[("__name__", "up")]), 1000, 1.0);
    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "absent")];
    let result = store.scan("t", &matchers, 0, 5000).await.unwrap();
    assert2::assert!(result.float_table.is_none());
    assert2::assert!(result.histogram_table.is_none());
}
