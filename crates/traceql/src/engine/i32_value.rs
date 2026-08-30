use super::*;

pub(crate) fn i32_value(batch: &RecordBatch, col: &str, row: usize) -> Result<i32> {
    Ok(batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?
        .as_primitive::<arrow::datatypes::Int32Type>()
        .value(row))
}
