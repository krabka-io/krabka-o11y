use super::{
    Aggregate, COL_TRACE_ID, ComparisonOp, Result, aggregate_expr_sql, aggregate_filter_sql,
    selector,
};

pub(crate) fn aggregate_filter_sql_query_any(
    spanset_sql: &str,
    agg: &Aggregate,
    op: ComparisonOp,
    value: f64,
) -> Result<String> {
    let trace = selector::ident(COL_TRACE_ID);
    let expr = match agg {
        Aggregate::Count => "COUNT(*)".to_string(),
        _ => aggregate_expr_sql(agg)?,
    };
    let pred = aggregate_filter_sql(&expr, op, value)?;
    Ok(format!(
        "WITH matched AS ({spanset_sql}), \
         passing AS (SELECT {trace} FROM matched GROUP BY {trace} HAVING {pred}) \
         SELECT matched.* FROM matched JOIN passing ON matched.{trace} = passing.{trace}"
    ))
}
