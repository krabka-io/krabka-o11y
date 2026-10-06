use super::{AsArray, RecordBatch, Result, TraceqlError};

pub(crate) fn i32_value(batch: &RecordBatch, col: &str, row: usize) -> Result<i32> {
    i32_value_column(batch.column_by_name(col), col, row)
}

pub(super) fn i32_value_column(
    column: Option<&arrow::array::ArrayRef>,
    col: &str,
    row: usize,
) -> Result<i32> {
    Ok(column
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?
        .as_primitive::<arrow::datatypes::Int32Type>()
        .value(row))
}
