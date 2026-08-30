use super::*;

#[tokio::test]
pub(crate) async fn label_values_returns_distinct_for_name() {
    let mut store = InMemoryMetricStore::new();
    store.push_float("t", lbls(&[("__name__", "up"), ("job", "a")]), 1, 1.0);
    store.push_float("t", lbls(&[("__name__", "up"), ("job", "b")]), 1, 1.0);
    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let values = store
        .label_values("t", "job", &matchers, 0, 10)
        .await
        .unwrap();
    assert2::assert!(values == vec!["a".to_string(), "b".to_string()]);
}
