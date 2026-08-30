use super::*;

/// Validates that the per-row evaluator supports a compare selection spanset.
///
/// The evaluator supports a selector whose `FieldExpr` is a boolean
/// combination of span, resource, and intrinsic comparisons. It also supports
/// a spanset-level `And` or `Or` of such selectors. This function rejects
/// structural operators and the parent, event, and link scopes.
pub(crate) fn validate_compare_selection(selection: &SpansetExpr) -> Result<()> {
    match selection {
        SpansetExpr::Selector(fe) => validate_compare_field_expr(fe),
        SpansetExpr::And(lhs, rhs) | SpansetExpr::Or(lhs, rhs) => {
            validate_compare_selection(lhs)?;
            validate_compare_selection(rhs)
        }
        SpansetExpr::Structural { .. } => Err(TraceqlError::Unsupported(
            "compare() selection does not support structural operators".into(),
        )),
    }
}
