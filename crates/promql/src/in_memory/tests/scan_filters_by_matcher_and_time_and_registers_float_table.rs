use super::*;

#[tokio::test]
pub(crate) async fn scan_filters_by_matcher_and_time_and_registers_float_table() {
    let mut store = InMemoryMetricStore::new();
    push_up_sample(&mut store, "a", 1000, 1.0);
    push_up_sample(&mut store, "a", 2000, 1.0);
    push_up_sample(&mut store, "b", 1000, 0.0);
    store.push_float(
        "t",
        lbls(&[crate::test_support::TestLabelPair::new("__name__", "down")]),
        1000,
        9.0,
    );

    let matchers = [
        LabelMatcher::new("__name__", MatchOp::Eq, "up"),
        LabelMatcher::new("job", MatchOp::Eq, "a"),
    ];
    let result = store.scan("t", &matchers, 0, 1500).await.unwrap();
    let table = result.float_table.clone().unwrap();
    assert2::assert!(result.histogram_table.is_none());
    assert2::assert!(count_rows(&result, &table).await == 1);
}
