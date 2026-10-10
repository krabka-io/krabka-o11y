use super::*;

#[tokio::test]
pub(crate) async fn query_shard_matcher_filters_by_series_fingerprint_modulo() {
    assert_query_shard_selects(MatchOp::Eq).await;
}
