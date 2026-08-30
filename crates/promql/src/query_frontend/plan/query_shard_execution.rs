use super::{QueryShardExecution, PromqlError, parse_promql, avg_partial_queries, moment_partial_queries, rank_reduction, expr_shard_reducer};

pub(crate) fn query_shard_execution(query: &str) -> Result<QueryShardExecution, PromqlError> {
    let expr = parse_promql(query)?;
    if let Some((sum_query, count_query)) = avg_partial_queries(&expr) {
        return Ok(QueryShardExecution::Avg {
            sum_query,
            count_query,
        });
    }
    if let Some((sum_query, count_query, sum_squares_query, kind)) = moment_partial_queries(&expr) {
        return Ok(QueryShardExecution::Moments {
            sum_query,
            count_query,
            sum_squares_query,
            kind,
        });
    }
    if let Some((k, kind, modifier)) = rank_reduction(&expr) {
        return Ok(QueryShardExecution::Rank { k, kind, modifier });
    }
    Ok(QueryShardExecution::Merge(expr_shard_reducer(&expr)))
}
