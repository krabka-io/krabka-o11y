use super::{
    Pipeline, Result, aggregate_rank_expr_sql, grouped_rank_pipeline_parts, grouped_rank_sql,
    is_search_preserving_aggregate, rank_limit,
};

pub(crate) fn grouped_rank_pipeline_sql(
    spanset_sql: &str,
    pipeline: &[Pipeline],
) -> Result<Option<String>> {
    let Some(parts) = grouped_rank_pipeline_parts(pipeline) else {
        return Ok(None);
    };
    if !is_search_preserving_aggregate(parts.aggregate) {
        return Ok(None);
    }
    Ok(Some(grouped_rank_sql(
        spanset_sql,
        parts.by,
        &aggregate_rank_expr_sql(parts.aggregate)?,
        rank_limit(parts.rank)?,
        parts.pre_filter,
        parts.post_filter,
    )?))
}
