use super::{BTreeSet, RecordBatch, TraceqlError, int32_value};

pub(crate) fn insert_i32_value(
    batch: &RecordBatch,
    row: usize,
    values: &mut BTreeSet<super::TypedValue>,
    column: &str,
) -> Result<(), TraceqlError> {
    values.insert(super::TypedValue::new(
        "int",
        int32_value(batch, column, row)?.to_string(),
    ));
    Ok(())
}
