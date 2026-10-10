use super::*;

/// Checks the `series` an `__query_shard__` matcher with `op` and `1_of_2`
/// selects out of twelve `up` series: `=` keeps the fingerprints that are even
/// (shard 1 of 2) and `!=` keeps the odd ones.
pub(crate) async fn assert_query_shard_selects(op: MatchOp) {
    let mut store = InMemoryMetricStore::new();
    let series = (0..12)
        .map(|id| lbls(&[("__name__", "up"), ("series", &id.to_string())]))
        .collect::<Vec<_>>();
    for labels in &series {
        store.push_float("t", labels.clone(), 1, 1.0);
    }

    let keeps_even = op == MatchOp::Eq;
    let expected = series
        .iter()
        .filter(|labels| labels.fingerprint().is_multiple_of(2) == keeps_even)
        .map(|labels| (labels.fingerprint(), labels.clone()))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();
    assert2::assert!(!expected.is_empty());
    assert2::assert!(expected.len() < series.len());

    let matchers = [
        LabelMatcher::new("__name__", MatchOp::Eq, "up"),
        LabelMatcher::new("__query_shard__", op, "1_of_2"),
    ];
    let got = store.series("t", &matchers, 0, 10).await.unwrap();

    assert2::assert!(got == expected);
}
