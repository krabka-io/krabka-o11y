use super::{Result, SpansetExpr, TraceqlError, validate_compare_field_expr};

/// Validates that the per-row evaluator supports a compare selection spanset.
///
/// The evaluator supports a selector whose `FieldExpr` is a boolean
/// combination of span, resource, and intrinsic comparisons. It also supports
/// a spanset-level `And` or `Or` of such selectors. This function rejects
/// structural operators and the parent, event, and link scopes.
pub(crate) fn validate_compare_selection(selection: &SpansetExpr) -> Result<()> {
    fn needs_nested_projection(field: &super::Field) -> bool {
        matches!(
            field.scope,
            super::Scope::Event
                | super::Scope::Link
                | super::Scope::Intrinsic(
                    super::Intrinsic::EventName
                        | super::Intrinsic::EventTimeSinceStart
                        | super::Intrinsic::LinkTraceId
                        | super::Intrinsic::LinkSpanId
                )
        )
    }
    fn reject_arithmetic(expr: &super::FieldExpr) -> Result<()> {
        match expr {
            super::FieldExpr::Field(_) => Err(TraceqlError::Unsupported(
                "compare selection requires typed boolean evaluation".into(),
            )),
            super::FieldExpr::ExpressionComparison { .. } => Err(TraceqlError::Unsupported(
                "compare() selection scalar arithmetic".into(),
            )),
            super::FieldExpr::And(lhs, rhs) | super::FieldExpr::Or(lhs, rhs) => {
                reject_arithmetic(lhs)?;
                reject_arithmetic(rhs)
            }
            super::FieldExpr::Not(inner) => reject_arithmetic(inner),
            super::FieldExpr::Comparison { lhs, .. } if needs_nested_projection(lhs) => Err(
                TraceqlError::Unsupported("compare selection needs nested projection".into()),
            ),
            super::FieldExpr::FieldComparison { lhs, rhs, .. }
                if needs_nested_projection(lhs) || needs_nested_projection(rhs) =>
            {
                Err(TraceqlError::Unsupported(
                    "compare selection needs nested projection".into(),
                ))
            }
            _ => Ok(()),
        }
    }
    match selection {
        SpansetExpr::Selector(fe) => {
            reject_arithmetic(fe)?;
            validate_compare_field_expr(fe)
        }
        SpansetExpr::And(lhs, rhs) | SpansetExpr::Or(lhs, rhs) => {
            validate_compare_selection(lhs)?;
            validate_compare_selection(rhs)
        }
        SpansetExpr::Structural { .. } => Err(TraceqlError::Unsupported(
            "compare() selection does not support structural operators".into(),
        )),
    }
}
