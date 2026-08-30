use super::*;

pub(crate) fn query_with_shard_selector(
    query: &str,
    shard: QueryShard,
) -> Result<String, PromqlError> {
    let mut expr = parse_promql(query)?;
    inject_shard_into_expr(&mut expr, shard);
    Ok(expr.to_string())
}
