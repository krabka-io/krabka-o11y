use super::{Array, AsArray, RecordBatch, Result, TraceqlError};

pub(crate) fn optional_fixed_8(
    batch: &RecordBatch,
    col: &str,
    row: usize,
) -> Result<Option<[u8; 8]>> {
    let arr = batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?;
    if arr.is_null(row) {
        return Ok(None);
    }
    arr.as_fixed_size_binary()
        .value(row)
        .try_into()
        .map(Some)
        .map_err(|_| TraceqlError::Exec(format!("column {col} is not 8 bytes")))
}
