use super::*;

pub(crate) fn grouped_rank_sql(
    spanset_sql: &str,
    by: &[Field],
    expr: &str,
    rank: RankLimit,
    pre_filter: Option<(ComparisonOp, f64)>,
    post_filter: Option<(ComparisonOp, f64)>,
) -> Result<String> {
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
    let direction = match rank.direction {
        RankDirection::Top => "DESC",
        RankDirection::Bottom => "ASC",
    };
    let having = if let Some((op, value)) = pre_filter {
        format!(" HAVING {}", aggregate_filter_sql(expr, op, value)?)
    } else {
        String::new()
    };
    let passing_source = if let Some((op, value)) = post_filter {
        let pred = aggregate_filter_sql("rank_value", op, value)?;
        format!("SELECT * FROM ranked WHERE {pred}")
    } else {
        "SELECT * FROM ranked".to_string()
    };
    Ok(format!(
        "WITH matched AS ({spanset_sql}), \
         ranked AS (SELECT {group_exprs}, {expr} AS rank_value FROM matched GROUP BY {group_exprs} \
                    {having} ORDER BY rank_value {direction} LIMIT {}), \
         passing AS ({passing_source}) \
         SELECT matched.* FROM matched JOIN passing ON {join_pred}",
        rank.k
    ))
}
