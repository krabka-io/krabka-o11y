use super::{RecordBatch, Result, fixed_bytes::fixed_bytes};

pub(crate) fn fixed_8(batch: &RecordBatch, col: &str, row: usize) -> Result<[u8; 8]> {
    fixed_bytes(batch, col, row)
}
