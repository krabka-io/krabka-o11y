use super::*;

#[test]
pub(crate) fn range_query_plan_expands_each_split_across_mimir_query_shards() {
    let plan = plan_range_query(
        "sum(rate(http_requests_total[5m]))",
        0,
        60_000,
        millis(60_000),
        QueryFrontendOptions {
            split_interval: millis(120_000),
            shard_count: 3,
        },
    )
    .unwrap();

    let shard_values = plan
        .iter()
        .map(|subquery| subquery.shard_matcher().expect("sharded subquery").value)
        .collect::<Vec<_>>();

    assert2::assert!(shard_values == vec!["1_of_3", "2_of_3", "3_of_3"]);
    assert2::assert!(
        plan.iter()
            .all(|subquery| subquery.start_ms == 0 && subquery.end_ms == 60_000)
    );

    let matcher = plan[0].shard_matcher().expect("first shard matcher");
    assert2::assert!(matcher.name.as_str() == "__query_shard__");
    assert2::assert!(matcher.op == MatchOp::Eq);
}
