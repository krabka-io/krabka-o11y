use super::*;

pub(crate) fn field_expr_to_sql_qualified(
    fe: &FieldExpr,
    span_alias: &str,
    parent_alias: &str,
) -> Result<String> {
    match fe {
        FieldExpr::Comparison { lhs, op, rhs } => {
            comparison_to_sql_qualified(lhs, *op, rhs, span_alias, parent_alias)
        }
        FieldExpr::And(a, b) => Ok(format!(
            "({} AND {})",
            field_expr_to_sql_qualified(a, span_alias, parent_alias)?,
            field_expr_to_sql_qualified(b, span_alias, parent_alias)?
        )),
        FieldExpr::Or(a, b) => Ok(format!(
            "({} OR {})",
            field_expr_to_sql_qualified(a, span_alias, parent_alias)?,
            field_expr_to_sql_qualified(b, span_alias, parent_alias)?
        )),
        FieldExpr::Not(inner) => Ok(format!(
            "(NOT {})",
            field_expr_to_sql_qualified(inner, span_alias, parent_alias)?
        )),
        FieldExpr::Field(field) => Ok(format!(
            "{} IS NOT NULL",
            qualified_field_ident(field, span_alias, parent_alias)
        )),
        FieldExpr::Const(value) => Ok(if *value {
            "TRUE".into()
        } else {
            "FALSE".into()
        }),
    }
}
