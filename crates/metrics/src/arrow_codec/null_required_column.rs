use super::*;

// cargo-mutants: error formatting is validated through required-column decode tests.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn null_required_column(column: &str, row: usize) -> HistogramCodecError {
    HistogramCodecError::SchemaMismatch(format!(
        "column `{column}` contains null for required row {row}"
    ))
}
