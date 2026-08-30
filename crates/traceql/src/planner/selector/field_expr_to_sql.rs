use super::*;

pub(crate) fn field_expr_to_sql(fe: &FieldExpr) -> Result<String> {
    match fe {
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
