use super::*;

#[test]
pub(crate) fn range_query_plan_shards_every_shardable_aggregate() {
    for query in [
        // `avg` reduces from partial `sum` and `count`.
        "avg(up)",
        // `group` reduces with its own aggregate reducer.
        "group(up)",
        // `min` and `max` reduce with their own aggregate reducers.
        "min(up)",
        "max(up)",
        // `stddev` and `stdvar` reduce from moment partials.
        "stddev(up)",
        "stdvar(up)",
        // `topk` and `bottomk` reduce by a final rank over the candidates.
        "topk(2, up)",
        "bottomk(2, up)",
    ] {
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

        assert2::assert!(plan.len() == 3, "{query}");
        assert2::assert!(
            plan.iter().all(|subquery| subquery.shard.is_some()),
            "{query}"
        );
    }
}
