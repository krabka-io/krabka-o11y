use super::{ComparisonOp, Field, GroupJoinSql, Result, aggregate_filter_sql, group_join_sql};

pub(crate) fn grouped_aggregate_sql(
    spanset_sql: &str,
    by: &[Field],
    filter: Option<(String, ComparisonOp, f64)>,
) -> Result<String> {
    let Some((expr, op, value)) = filter else {
        return Ok(format!("SELECT * FROM ({spanset_sql}) AS q"));
    };
    let GroupJoinSql {
        group_exprs,
        join_pred,
    } = group_join_sql(by);
    let pred = aggregate_filter_sql(&expr, op, value)?;
    Ok(format!(
        "WITH matched AS ({spanset_sql}), \
         passing AS (SELECT {group_exprs} FROM matched GROUP BY {group_exprs} HAVING {pred}) \
         SELECT matched.* FROM matched JOIN passing ON {join_pred}"
    ))
}
