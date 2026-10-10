use super::*;

#[tokio::test]
pub(crate) async fn query_shard_neq_matcher_excludes_matching_fingerprint_modulo() {
    assert_query_shard_selects(MatchOp::Neq).await;
}
