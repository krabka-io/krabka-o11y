use super::*;

#[tokio::test]
pub(crate) async fn query_shard_neq_matcher_excludes_matching_fingerprint_modulo() {
    let mut store = InMemoryMetricStore::new();
    let series = (0..12)
        .map(|id| lbls(&[("__name__", "up"), ("series", &id.to_string())]))
        .collect::<Vec<_>>();
    for labels in &series {
        store.push_float("t", labels.clone(), 1, 1.0);
    }

    let expected = series
        .iter()
        .filter(|labels| labels.fingerprint() % 2 != 0)
        .map(|labels| (labels.fingerprint(), labels.clone()))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();
    assert2::assert!(!expected.is_empty());
    assert2::assert!(expected.len() < series.len());

    let matchers = [
        LabelMatcher::new("__name__", MatchOp::Eq, "up"),
        LabelMatcher::new("__query_shard__", MatchOp::Neq, "1_of_2"),
    ];
    let got = store.series("t", &matchers, 0, 10).await.unwrap();

    assert2::assert!(got == expected);
}
