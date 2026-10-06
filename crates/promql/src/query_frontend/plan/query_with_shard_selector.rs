use super::{PromqlError, QueryShard, inject_shard_into_expr, parse_promql};
use crate::format_promql_expr;

pub(crate) fn query_with_shard_selector(
    query: &str,
    shard: QueryShard,
) -> Result<String, PromqlError> {
    let mut expr = parse_promql(query)?;
    inject_shard_into_expr(&mut expr, shard);
    Ok(format_promql_expr(&expr))
}
