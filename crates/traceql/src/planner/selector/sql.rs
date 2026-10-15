use super::{
    ComparisonOp, Field, FieldExpr, QualifiedComparison, Result, Scope, TraceqlError, Value,
    anchored, comparison_to_sql, comparison_to_sql_qualified, comparison_value_sql,
    field_to_column, ident, qualified_field_ident, string_lit,
};

/// A comparison of an already-quoted column expression against a value.
#[derive(Clone, Copy)]
pub(crate) struct ColumnComparison<'a> {
    pub(crate) col: &'a str,
    /// Decides how the comparison value is typed.
    pub(crate) field: &'a Field,
    pub(crate) op: ComparisonOp,
    pub(crate) operand: &'a Value,
}

/// Renders `col <op> operand`.
pub(crate) fn column_comparison_sql(comparison: ColumnComparison<'_>) -> Result<String> {
    let ColumnComparison {
        col,
        field,
        op,
        operand: value,
    } = comparison;
    Ok(
        match (ColumnComparison {
            col,
            field,
            op,
            operand: value,
        }) {
            ColumnComparison {
                op: ComparisonOp::Eq,
                operand: Value::Nil,
                ..
            } => format!("{col} IS NULL"),
            ColumnComparison {
                op: ComparisonOp::Neq,
                operand: Value::Nil,
                ..
            } => format!("{col} IS NOT NULL"),
            ColumnComparison {
                op: ComparisonOp::Re,
                operand: Value::Str(pattern),
                ..
            } => {
                format!("regexp_like({col}, {})", string_lit(&anchored(pattern)))
            }
            ColumnComparison {
                op: ComparisonOp::Nre,
                operand: Value::Str(pattern),
                ..
            } => {
                format!("NOT regexp_like({col}, {})", string_lit(&anchored(pattern)))
            }
            ColumnComparison {
                op: ComparisonOp::Eq,
                operand: v,
                ..
            } => format!("{col} = {}", comparison_value_sql(field, v)?),
            ColumnComparison {
                op: ComparisonOp::Neq,
                operand: v,
                ..
            } => format!("{col} != {}", comparison_value_sql(field, v)?),
            ColumnComparison {
                op: ComparisonOp::Lt,
                operand: v,
                ..
            } => format!("{col} < {}", comparison_value_sql(field, v)?),
            ColumnComparison {
                op: ComparisonOp::Lte,
                operand: v,
                ..
            } => format!("{col} <= {}", comparison_value_sql(field, v)?),
            ColumnComparison {
                op: ComparisonOp::Gt,
                operand: v,
                ..
            } => format!("{col} > {}", comparison_value_sql(field, v)?),
            ColumnComparison {
                op: ComparisonOp::Gte,
                operand: v,
                ..
            } => format!("{col} >= {}", comparison_value_sql(field, v)?),
            ColumnComparison {
                op: ComparisonOp::Re | ComparisonOp::Nre,
                ..
            } => {
                return Err(TraceqlError::Plan(
                    "regex comparison requires string value".into(),
                ));
            }
        },
    )
}

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

pub(crate) fn field_expr_to_sql_qualified(
    fe: &FieldExpr,
    span_alias: &str,
    parent_alias: &str,
) -> Result<String> {
    // A dynamic field comparison requires the whole predicate to read packed
    // values: a heterogeneous promoted sibling may be NULL despite its presence.
    if fe.has_field_comparison() {
        return Ok(format!(
            "{span_alias}.{}",
            ident(&crate::ast::field_comparison_column(fe))
        ));
    }
    match fe {
        FieldExpr::ExpressionComparison { .. } | FieldExpr::FieldComparison { .. } => Ok(format!(
            "{span_alias}.{}",
            ident(&crate::ast::field_comparison_column(fe))
        )),
        FieldExpr::Comparison { lhs, op, rhs } => {
            comparison_to_sql_qualified(&QualifiedComparison {
                field: lhs,
                op: *op,
                operand: rhs,
                span_alias,
                parent_alias,
            })
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
        (None, None, ParentPredicateOperator::And | ParentPredicateOperator::Or)
        | (Some(_), None, ParentPredicateOperator::Or)
        | (None, Some(_), ParentPredicateOperator::Or) => None,
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
