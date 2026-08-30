use super::*;

pub(crate) fn fixed_8(batch: &RecordBatch, col: &str, row: usize) -> Result<[u8; 8]> {
    batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?
        .as_fixed_size_binary()
        .value(row)
        .try_into()
        .map_err(|_| TraceqlError::Exec(format!("column {col} is not 8 bytes")))
}
