use super::*;

pub(crate) fn fixed_array_value<const N: usize>(
    values: &FixedSizeBinaryArray,
    row: usize,
    name: &str,
) -> Result<[u8; N], TraceqlError> {
    if values.is_null(row) {
        return Ok([0; N]);
    }
    values
        .value(row)
        .try_into()
        .map_err(|_| TraceqlError::Store(format!("bad fixed binary width for `{name}`")))
}
