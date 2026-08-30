use super::HistogramCodecError;

// cargo-mutants: exercised through the sample and histogram codec decode tests.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn schema_mismatch(column: &str) -> HistogramCodecError {
    HistogramCodecError::SchemaMismatch(format!("column `{column}` missing or wrong type"))
}
