use super::{Array, HistogramCodecError, null_required_column};

// cargo-mutants: exercised through the sample and histogram codec null-column tests.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn require_non_null(
    array: &dyn Array,
    row: usize,
    column: &str,
) -> Result<(), HistogramCodecError> {
    if array.is_null(row) {
        Err(null_required_column(column, row))
    } else {
        Ok(())
    }
}
