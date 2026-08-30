use super::{RecordBatch, HistogramCodecError, Array, schema_mismatch};

// cargo-mutants: generic downcast glue is covered by caller-specific schema tests.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn typed_column<'a, T: 'static>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a T, HistogramCodecError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| schema_mismatch(name))?
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| schema_mismatch(name))
}
