use super::*;

#[yare::parameterized(
    nested_average = { "avg(sum by (job)(up))" },
    unsupported_quantile = { "quantile(0.9, up)" },
)]
fn range_query_plan_skips_shards_for_unsupported_reducers(query: &str) {
    let plan = plan_range_query(
        query,
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
