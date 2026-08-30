use super::*;

pub(crate) fn query_supports_frontend_sharding(query: &str) -> Result<bool, PromqlError> {
    let expr = parse_promql(query)?;
    Ok(avg_partial_queries(&expr).is_some()
        || moment_partial_queries(&expr).is_some()
        || rank_reduction(&expr).is_some()
        || expr_supports_frontend_sharding(&expr))
}
