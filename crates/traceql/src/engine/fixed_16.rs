use super::{RecordBatch, Result, fixed_bytes::fixed_bytes};

pub(crate) fn fixed_16(batch: &RecordBatch, col: &str, row: usize) -> Result<[u8; 16]> {
    fixed_bytes(batch, col, row)
}
