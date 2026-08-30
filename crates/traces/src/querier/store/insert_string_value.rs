use super::*;

pub(crate) fn insert_string_value(
    batch: &RecordBatch,
    row: usize,
    values: &mut BTreeSet<(String, String)>,
    column: &str,
) -> Result<(), TraceqlError> {
    values.insert(("string".to_string(), string_value(batch, column, row)?));
    Ok(())
}
