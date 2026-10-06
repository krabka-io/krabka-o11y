use super::{FieldExpr, Result, comparison_to_sql, field_to_column, ident};

pub(crate) fn field_expr_to_sql(fe: &FieldExpr) -> Result<String> {
    // A dynamic field comparison requires the whole predicate to read packed
    // values: a heterogeneous promoted sibling may be NULL despite its presence.
    if fe.has_field_comparison() {
        return Ok(ident(&crate::ast::field_comparison_column(fe)));
    }
    match fe {
        FieldExpr::ExpressionComparison { .. } | FieldExpr::FieldComparison { .. } => {
            Ok(ident(&crate::ast::field_comparison_column(fe)))
        }
        FieldExpr::Comparison { lhs, op, rhs } => comparison_to_sql(lhs, *op, rhs),
        FieldExpr::And(a, b) => Ok(format!(
            "({} AND {})",
            field_expr_to_sql(a)?,
            field_expr_to_sql(b)?
        )),
        FieldExpr::Or(a, b) => Ok(format!(
            "({} OR {})",
            field_expr_to_sql(a)?,
            field_expr_to_sql(b)?
        )),
        FieldExpr::Not(inner) => Ok(format!("(NOT {})", field_expr_to_sql(inner)?)),
        FieldExpr::Field(field) => Ok(format!("{} IS NOT NULL", ident(&field_to_column(field)))),
        // `{}` / `{ true }` => match every span; `{ false }` => match none.
        FieldExpr::Const(value) => Ok(if *value {
            "TRUE".into()
        } else {
            "FALSE".into()
        }),
    }
}
