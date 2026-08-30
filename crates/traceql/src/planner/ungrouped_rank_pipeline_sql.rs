use super::*;

pub(crate) fn ungrouped_rank_pipeline_sql(spanset_sql: &str, pipeline: &[Pipeline]) -> Result<Option<String>> {
    let Some((agg, rank, filter)) = ungrouped_rank_pipeline_parts(pipeline) else {
        return Ok(None);
    };
    if !is_search_preserving_aggregate(agg) {
        return Ok(None);
    }
    let rank = rank_limit(rank)?;
    if rank.k == 0 {
        return Ok(Some(ungrouped_rank_sql(spanset_sql, rank)));
    }
    let Some((op, value)) = filter else {
        return Ok(Some(ungrouped_rank_sql(spanset_sql, rank)));
    };
    Ok(Some(aggregate_filter_sql_query_any(
        spanset_sql,
        agg,
        op,
        value,
    )?))
}
