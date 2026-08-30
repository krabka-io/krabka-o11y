use super::*;

#[test]
pub(crate) fn range_query_plan_skips_nested_avg_until_rewrite_is_aggregate_aware() {
    let plan = plan_range_query(
        "avg(sum by (job)(up))",
        0,
        60_000,
        millis(60_000),
        QueryFrontendOptions {
            split_interval: millis(120_000),
            shard_count: 3,
        },
    )
    .unwrap();

    assert2::assert!(plan.len() == 1);
    assert2::assert!(plan[0].shard == None);
}
