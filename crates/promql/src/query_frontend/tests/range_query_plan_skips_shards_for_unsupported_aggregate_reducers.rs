use super::*;

#[test]
pub(crate) fn range_query_plan_skips_shards_for_unsupported_aggregate_reducers() {
    let plan = plan_range_query(
        "quantile(0.9, up)",
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
