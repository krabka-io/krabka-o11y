use super::*;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ShardCall<'a> {
    pub(crate) query: &'a str,
    pub(crate) shard: Option<QueryShard>,
}

/// Each recorded call's query text and shard, in call order.
pub(crate) fn shard_calls(calls: &[FrontendRangeQuery]) -> Vec<ShardCall<'_>> {
    calls
        .iter()
        .map(|query| ShardCall {
            query: query.query.as_str(),
            shard: query.shard,
        })
        .collect()
}

/// Each of `queries` run on shard 1 and then shard 2 of 2.
pub(crate) fn on_both_shards<'a>(queries: &[&'a str]) -> Vec<ShardCall<'a>> {
    queries
        .iter()
        .flat_map(|&query| {
            [1, 2].map(|index| ShardCall {
                query,
                shard: Some(QueryShard { index, total: 2 }),
            })
        })
        .collect()
}
