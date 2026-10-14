use super::{ComparisonOp, Field, GroupJoinSql, Result, aggregate_filter_sql, group_join_sql};

pub(crate) struct AggregateFilter {
    pub(crate) expression: String,
    pub(crate) op: ComparisonOp,
    pub(crate) value: f64,
}

pub(crate) fn grouped_aggregate_sql(
    spanset_sql: &str,
    by: &[Field],
    filter: Option<AggregateFilter>,
) -> Result<String> {
    let Some(filter) = filter else {
        return Ok(format!("SELECT * FROM ({spanset_sql}) AS q"));
    };
    let GroupJoinSql {
        group_exprs,
        join_pred,
    } = group_join_sql(by);
    let pred = aggregate_filter_sql(&filter.expression, filter.op, filter.value)?;
    Ok(format!(
        "WITH matched AS ({spanset_sql}), \
         passing AS (SELECT {group_exprs} FROM matched GROUP BY {group_exprs} HAVING {pred}) \
         SELECT matched.* FROM matched JOIN passing ON {join_pred}"
    ))
}
