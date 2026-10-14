use super::{Aggregate, ComparisonOp, Result, aggregate_expr_sql, aggregate_filter_query_any_sql};

pub(crate) fn aggregate_filter_sql_query_any(
    spanset_sql: &str,
    agg: &Aggregate,
    op: ComparisonOp,
    value: f64,
) -> Result<String> {
    let expr = match agg {
        Aggregate::Count => "COUNT(*)".to_string(),
        _ => aggregate_expr_sql(agg)?,
    };
    aggregate_filter_query_any_sql(spanset_sql, &expr, op, value)
}
