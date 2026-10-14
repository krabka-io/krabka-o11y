use super::{BTreeSet, RecordBatch, TraceqlError, string_value};

pub(crate) fn insert_string_value(
    batch: &RecordBatch,
    row: usize,
    values: &mut BTreeSet<super::TypedValue>,
    column: &str,
) -> Result<(), TraceqlError> {
    super::insert_typed_value(values, "string", string_value(batch, column, row)?);
    Ok(())
}
