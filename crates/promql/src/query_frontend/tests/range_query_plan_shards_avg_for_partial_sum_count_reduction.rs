use super::*;

#[test]
pub(crate) fn range_query_plan_shards_avg_for_partial_sum_count_reduction() {
    let plan = plan_range_query(
        "avg(up)",
        0,
        60_000,
        millis(60_000),
        QueryFrontendOptions {
            split_interval: millis(120_000),
            shard_count: 3,
        },
    )
    .unwrap();

    assert2::assert!(plan.len() == 3);
    assert2::assert!(plan.iter().all(|subquery| subquery.shard.is_some()));
}
