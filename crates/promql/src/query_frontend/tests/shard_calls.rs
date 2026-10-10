use super::*;

/// Each recorded call's query text and shard, in call order.
pub(crate) fn shard_calls(calls: &[FrontendRangeQuery]) -> Vec<(&str, Option<QueryShard>)> {
    calls
        .iter()
        .map(|query| (query.query.as_str(), query.shard))
        .collect()
}

/// Each of `queries` run on shard 1 and then shard 2 of 2.
pub(crate) fn on_both_shards<'a>(queries: &[&'a str]) -> Vec<(&'a str, Option<QueryShard>)> {
    queries
        .iter()
        .flat_map(|&query| [1, 2].map(|index| (query, Some(QueryShard { index, total: 2 }))))
        .collect()
}
