use super::*;

pub(crate) fn grouped_rank_pipeline_sql(spanset_sql: &str, pipeline: &[Pipeline]) -> Result<Option<String>> {
    let Some((agg, by, rank, pre_filter, post_filter)) = grouped_rank_pipeline_parts(pipeline)
    else {
        return Ok(None);
    };
    if !is_search_preserving_aggregate(agg) {
        return Ok(None);
    }
    Ok(Some(grouped_rank_sql(
        spanset_sql,
        by,
        &aggregate_rank_expr_sql(agg)?,
        rank_limit(rank)?,
        pre_filter,
        post_filter,
    )?))
}
