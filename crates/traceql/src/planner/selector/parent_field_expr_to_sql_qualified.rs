use super::{
    FieldExpr, QualifiedComparison, Result, Scope, comparison_to_sql_qualified,
    qualified_field_ident,
};

struct ParentPredicateSides {
    left: Option<String>,
    right: Option<String>,
}

enum ParentPredicateOperator {
    And,
    Or,
}

fn combine_parent_predicates(
    sides: ParentPredicateSides,
    operator: ParentPredicateOperator,
) -> Option<String> {
    let ParentPredicateSides { left, right } = sides;
    match (left, right, operator) {
        (Some(left), Some(right), ParentPredicateOperator::And) => {
            Some(format!("({left} AND {right})"))
        }
        (Some(left), Some(right), ParentPredicateOperator::Or) => {
            Some(format!("({left} OR {right})"))
        }
        (Some(predicate), None, ParentPredicateOperator::And)
        | (None, Some(predicate), ParentPredicateOperator::And) => Some(predicate),
        (None, None, ParentPredicateOperator::And)
        | (Some(_), None, ParentPredicateOperator::Or)
        | (None, Some(_), ParentPredicateOperator::Or)
        | (None, None, ParentPredicateOperator::Or) => None,
    }
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
            Ok(combine_parent_predicates(
                ParentPredicateSides { left, right },
                ParentPredicateOperator::And,
            ))
        }
        FieldExpr::Or(a, b) => {
            let left = parent_field_expr_to_sql_qualified(a, span_alias, parent_alias)?;
            let right = parent_field_expr_to_sql_qualified(b, span_alias, parent_alias)?;
            Ok(combine_parent_predicates(
                ParentPredicateSides { left, right },
                ParentPredicateOperator::Or,
            ))
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
