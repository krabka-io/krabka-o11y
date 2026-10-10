use super::{Field, selector};

/// The SQL that groups `matched` rows by the `by` fields and joins them back
/// to the `passing` groups.
pub(crate) struct GroupJoinSql {
    /// The comma-separated grouping columns, for `SELECT` and `GROUP BY`.
    pub(crate) group_exprs: String,
    /// The `matched.col = passing.col AND ...` join predicate.
    pub(crate) join_pred: String,
}

pub(crate) fn group_join_sql(by: &[Field]) -> GroupJoinSql {
    let group_cols = by
        .iter()
        .map(|field| selector::ident(&selector::field_to_column(field)))
        .collect::<Vec<_>>();
    let join_pred = group_cols
        .iter()
        .map(|col| format!("matched.{col} = passing.{col}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    GroupJoinSql {
        group_exprs: group_cols.join(", "),
        join_pred,
    }
}
