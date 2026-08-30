use super::{RecordBatch, TraceqlError, fixed};

pub(crate) fn fixed_value<const N: usize>(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<[u8; N], TraceqlError> {
    fixed(batch, name)?
        .value(row)
        .try_into()
        .map_err(|_| TraceqlError::Store(format!("bad fixed binary width for `{name}`")))
}
