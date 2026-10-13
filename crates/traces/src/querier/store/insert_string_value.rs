use super::{BTreeSet, RecordBatch, TraceqlError, string_value};

pub(crate) fn insert_string_value(
    batch: &RecordBatch,
    row: usize,
    values: &mut BTreeSet<super::TypedValue>,
    column: &str,
) -> Result<(), TraceqlError> {
    values.insert(super::TypedValue::new(
        "string",
        string_value(batch, column, row)?,
    ));
    Ok(())
}
