use super::*;

pub(crate) fn aggregate_filter_sql_query(
    spanset_sql: &str,
    agg: &Aggregate,
    op: ComparisonOp,
    value: f64,
) -> Result<String> {
    aggregate_filter_sql_query_any(spanset_sql, agg, op, value)
}
