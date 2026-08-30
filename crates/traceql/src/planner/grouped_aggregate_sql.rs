use super::{ComparisonOp, Field, Result, aggregate_filter_sql, selector};

pub(crate) fn grouped_aggregate_sql(
    spanset_sql: &str,
    by: &[Field],
    filter: Option<(String, ComparisonOp, f64)>,
) -> Result<String> {
    let Some((expr, op, value)) = filter else {
        return Ok(format!("SELECT * FROM ({spanset_sql}) AS q"));
    };
    let group_cols = by
        .iter()
        .map(|field| selector::ident(&selector::field_to_column(field)))
        .collect::<Vec<_>>();
    let group_exprs = group_cols.join(", ");
    let join_pred = group_cols
        .iter()
        .map(|col| format!("matched.{col} = passing.{col}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let pred = aggregate_filter_sql(&expr, op, value)?;
    Ok(format!(
        "WITH matched AS ({spanset_sql}), \
         passing AS (SELECT {group_exprs} FROM matched GROUP BY {group_exprs} HAVING {pred}) \
         SELECT matched.* FROM matched JOIN passing ON {join_pred}"
    ))
}
