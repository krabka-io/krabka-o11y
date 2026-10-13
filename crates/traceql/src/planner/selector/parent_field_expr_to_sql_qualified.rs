use super::{
    FieldExpr, QualifiedComparison, Result, Scope, comparison_to_sql_qualified,
    qualified_field_ident,
};

struct ParentPredicateSides {
    left: Option<String>,
    right: Option<String>,
}

pub(crate) fn parent_field_expr_to_sql_qualified(
    fe: &FieldExpr,
    span_alias: &str,
    parent_alias: &str,
) -> Result<Option<String>> {
    match fe {
        FieldExpr::Comparison { lhs, op, rhs } if matches!(lhs.scope, Scope::Parent) => {
            Ok(Some(comparison_to_sql_qualified(&QualifiedComparison {
                field: lhs,
                op: *op,
                operand: rhs,
                span_alias,
                parent_alias,
            })?))
        }
        FieldExpr::Field(field) if matches!(field.scope, Scope::Parent) => Ok(Some(format!(
            "{} IS NOT NULL",
            qualified_field_ident(field, span_alias, parent_alias)
        ))),
        FieldExpr::And(a, b) => {
            let left = parent_field_expr_to_sql_qualified(a, span_alias, parent_alias)?;
            let right = parent_field_expr_to_sql_qualified(b, span_alias, parent_alias)?;
            Ok(match (ParentPredicateSides { left, right }) {
                ParentPredicateSides {
                    left: Some(left),
                    right: Some(right),
                } => Some(format!("({left} AND {right})")),
                ParentPredicateSides {
                    left: Some(predicate),
                    right: None,
                }
                | ParentPredicateSides {
                    left: None,
                    right: Some(predicate),
                } => Some(predicate),
                ParentPredicateSides {
                    left: None,
                    right: None,
                } => None,
            })
        }
        FieldExpr::Or(a, b) => {
            let left = parent_field_expr_to_sql_qualified(a, span_alias, parent_alias)?;
            let right = parent_field_expr_to_sql_qualified(b, span_alias, parent_alias)?;
            Ok(match (ParentPredicateSides { left, right }) {
                ParentPredicateSides {
                    left: Some(left),
                    right: Some(right),
                } => Some(format!("({left} OR {right})")),
                ParentPredicateSides {
                    left: Some(_) | None,
                    right: None,
                }
                | ParentPredicateSides {
                    left: None,
                    right: Some(_),
                } => None,
            })
        }
        FieldExpr::Not(inner) => {
            Ok(
                parent_field_expr_to_sql_qualified(inner, span_alias, parent_alias)?
                    .map(|predicate| format!("(NOT {predicate})")),
            )
        }
        FieldExpr::ExpressionComparison { .. }
        | FieldExpr::FieldComparison { .. }
        | FieldExpr::Comparison { .. }
        | FieldExpr::Field(_)
        | FieldExpr::Const(_) => Ok(None),
    }
}
