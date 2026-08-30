use super::*;

pub(crate) fn i64_value(batch: &RecordBatch, col: &str, row: usize) -> Result<i64> {
    Ok(batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?
        .as_primitive::<arrow::datatypes::Int64Type>()
        .value(row))
}
