use super::{ComparisonOp, Field, Result, Value, column_comparison_sql, field_to_column, ident};

pub(crate) fn comparison_to_sql(field: &Field, op: ComparisonOp, value: &Value) -> Result<String> {
    column_comparison_sql(&ident(&field_to_column(field)), field, op, value)
}
