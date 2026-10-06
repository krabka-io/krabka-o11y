use super::{AsArray, RecordBatch, Result, TraceqlError};

pub(crate) fn fixed_8(batch: &RecordBatch, col: &str, row: usize) -> Result<[u8; 8]> {
    fixed_8_column(batch.column_by_name(col), col, row)
}

pub(super) fn fixed_8_column(
    column: Option<&arrow::array::ArrayRef>,
    col: &str,
    row: usize,
) -> Result<[u8; 8]> {
    column
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?
        .as_fixed_size_binary()
        .value(row)
        .try_into()
        .map_err(|_| TraceqlError::Exec(format!("column {col} is not 8 bytes")))
}
