use super::FieldExpr;

// The full expression is the key: do not hash and risk aliasing two predicates.
pub(crate) fn field_comparison_column(expr: &FieldExpr) -> String {
    format!("__traceql_field_comparison_{expr:?}")
}
