use super::{ComparisonOp, Field, Result, Value, column_comparison_sql, qualified_field_ident};

pub(crate) fn comparison_to_sql_qualified(
    field: &Field,
    op: ComparisonOp,
    value: &Value,
    span_alias: &str,
    parent_alias: &str,
) -> Result<String> {
    let col = qualified_field_ident(field, span_alias, parent_alias);
    column_comparison_sql(&col, field, op, value)
}
