use super::*;

pub(crate) fn nullable_fixed_value<const N: usize>(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<Option<[u8; N]>, TraceqlError> {
    let arr = fixed(batch, name)?;
    if arr.is_null(row) {
        Ok(None)
    } else {
        arr.value(row)
            .try_into()
            .map(Some)
            .map_err(|_| TraceqlError::Store(format!("bad fixed binary width for `{name}`")))
    }
}
