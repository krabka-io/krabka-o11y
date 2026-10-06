use super::{Array, AsArray, Result, TraceqlError};

pub(super) fn optional_fixed_8_column(
    column: Option<&arrow::array::ArrayRef>,
    col: &str,
    row: usize,
) -> Result<Option<[u8; 8]>> {
    let arr = column.ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?;
    if arr.is_null(row) {
        return Ok(None);
    }
    arr.as_fixed_size_binary()
        .value(row)
        .try_into()
        .map(Some)
        .map_err(|_| TraceqlError::Exec(format!("column {col} is not 8 bytes")))
}
