use super::{
    PromqlError, QueryShardExecution, avg_partial_queries, expr_shard_reducer,
    moment_partial_queries, parse_promql, rank_reduction,
};

pub(crate) fn query_shard_execution(query: &str) -> Result<QueryShardExecution, PromqlError> {
    let expr = parse_promql(query)?;
    if let Some(partials) = avg_partial_queries(&expr) {
        return Ok(QueryShardExecution::Avg {
            sum_query: partials.sum,
            count_query: partials.count,
        });
    }
    if let Some(partials) = moment_partial_queries(&expr) {
        return Ok(QueryShardExecution::Moments {
            sum_query: partials.sum,
            count_query: partials.count,
            sum_squares_query: partials.sum_squares,
            kind: partials.reduction,
        });
    }
    if let Some(reduction) = rank_reduction(&expr) {
        return Ok(QueryShardExecution::Rank {
            k: reduction.k,
            kind: reduction.kind,
            modifier: reduction.modifier,
        });
    }
    Ok(QueryShardExecution::Merge(expr_shard_reducer(&expr)))
}
