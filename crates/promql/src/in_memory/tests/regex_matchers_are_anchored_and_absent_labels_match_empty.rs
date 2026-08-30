use super::*;

#[tokio::test]
pub(crate) async fn regex_matchers_are_anchored_and_absent_labels_match_empty() {
    let mut store = InMemoryMetricStore::new();
    store.push_float("t", lbls(&[("__name__", "up"), ("job", "api")]), 1, 1.0);
    store.push_float("t", lbls(&[("__name__", "up")]), 1, 2.0);

    let anchored = [LabelMatcher::new("job", MatchOp::Re, "a")];
    assert2::assert!(
        store
            .scan("t", &anchored, 0, 10)
            .await
            .unwrap()
            .float_table
            .is_none()
    );

    let empty = [LabelMatcher::new("missing", MatchOp::Eq, "")];
    assert2::assert!(
        store
            .scan("t", &empty, 0, 10)
            .await
            .unwrap()
            .float_table
            .is_some()
    );
}
