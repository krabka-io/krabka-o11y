use super::*;

#[tokio::test]
pub(crate) async fn scan_validates_regex_matchers_before_row_iteration() {
    let store = InMemoryMetricStore::new();
    let matchers = [LabelMatcher::new("__name__", MatchOp::Re, "[")];

    let Err(error) = store.scan("missing", &matchers, 0, 5000).await else {
        panic!("expected invalid regex to fail before scanning rows");
    };

    assert2::assert!(matches!(error, PromqlError::Plan(_)));
    assert2::assert!(error.to_string().contains("bad regex"));
}
