use super::*;

#[test]
pub(crate) fn range_query_plan_shards_topk_and_bottomk_for_final_rank_reduction() {
    for query in ["topk(2, up)", "bottomk(2, up)"] {
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

        assert2::assert!(plan.len() == 3);
        assert2::assert!(plan.iter().all(|subquery| subquery.shard.is_some()));
    }
}
