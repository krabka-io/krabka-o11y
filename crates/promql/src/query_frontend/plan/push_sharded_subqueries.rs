use super::{FrontendRangeQuery, QueryShard, Time};

pub(crate) fn push_sharded_subqueries(
    subqueries: &mut Vec<FrontendRangeQuery>,
    query: &str,
    start_ms: i64,
    end_ms: i64,
    step: Time,
    shard_count: usize,
) {
    if shard_count == 1 {
        subqueries.push(FrontendRangeQuery {
            query: query.to_string(),
            start_ms,
            end_ms,
            step,
            shard: None,
        });
        return;
    }

    for index in 1..=shard_count {
        subqueries.push(FrontendRangeQuery {
            query: query.to_string(),
            start_ms,
            end_ms,
            step,
            shard: Some(QueryShard {
                index,
                total: shard_count,
            }),
        });
    }
}
