use super::{BTreeSet, RecordBatch, TraceqlError, int64_value};

pub(crate) fn insert_i64_value(
    batch: &RecordBatch,
    row: usize,
    values: &mut BTreeSet<super::TypedValue>,
    type_: &str,
    column: &str,
) -> Result<(), TraceqlError> {
    values.insert(super::TypedValue::new(
        type_,
        int64_value(batch, column, row)?.to_string(),
    ));
    Ok(())
}
