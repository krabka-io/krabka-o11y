use super::{AsArray, RecordBatch, Result, TraceqlError};

pub(crate) fn fixed_16(batch: &RecordBatch, col: &str, row: usize) -> Result<[u8; 16]> {
    fixed_16_column(batch.column_by_name(col), col, row)
}

pub(super) fn fixed_16_column(
    column: Option<&arrow::array::ArrayRef>,
    col: &str,
    row: usize,
) -> Result<[u8; 16]> {
    column
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?
        .as_fixed_size_binary()
        .value(row)
        .try_into()
        .map_err(|_| TraceqlError::Exec(format!("column {col} is not 16 bytes")))
}
