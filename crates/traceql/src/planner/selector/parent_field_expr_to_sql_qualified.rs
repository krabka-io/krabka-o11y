use super::{FieldExpr, Result, Scope, comparison_to_sql_qualified, qualified_field_ident};

pub(crate) fn parent_field_expr_to_sql_qualified(
    fe: &FieldExpr,
    span_alias: &str,
    parent_alias: &str,
) -> Result<Option<String>> {
    match fe {
        FieldExpr::Comparison { lhs, op, rhs } if matches!(lhs.scope, Scope::Parent) => Ok(Some(
            comparison_to_sql_qualified(lhs, *op, rhs, span_alias, parent_alias)?,
        )),
        FieldExpr::Field(field) if matches!(field.scope, Scope::Parent) => Ok(Some(format!(
            "{} IS NOT NULL",
            qualified_field_ident(field, span_alias, parent_alias)
        ))),
        FieldExpr::And(a, b) => {
            let left = parent_field_expr_to_sql_qualified(a, span_alias, parent_alias)?;
            let right = parent_field_expr_to_sql_qualified(b, span_alias, parent_alias)?;
            Ok(match (left, right) {
                (Some(left), Some(right)) => Some(format!("({left} AND {right})")),
                (Some(predicate), None) | (None, Some(predicate)) => Some(predicate),
                (None, None) => None,
            })
        }
        FieldExpr::Or(a, b) => {
            let left = parent_field_expr_to_sql_qualified(a, span_alias, parent_alias)?;
            let right = parent_field_expr_to_sql_qualified(b, span_alias, parent_alias)?;
            Ok(match (left, right) {
                (Some(left), Some(right)) => Some(format!("({left} OR {right})")),
                (Some(_) | None, None) | (None, Some(_)) => None,
            })
        }
        FieldExpr::Not(inner) => {
            Ok(
                parent_field_expr_to_sql_qualified(inner, span_alias, parent_alias)?
                    .map(|predicate| format!("(NOT {predicate})")),
            )
        }
        FieldExpr::Comparison { .. } | FieldExpr::Field(_) | FieldExpr::Const(_) => Ok(None),
    }
}
