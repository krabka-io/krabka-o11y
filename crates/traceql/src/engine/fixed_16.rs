use super::{AsArray, RecordBatch, Result, TraceqlError};

pub(crate) fn fixed_16(batch: &RecordBatch, col: &str, row: usize) -> Result<[u8; 16]> {
    batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?
        .as_fixed_size_binary()
        .value(row)
        .try_into()
        .map_err(|_| TraceqlError::Exec(format!("column {col} is not 16 bytes")))
}
