use super::{BTreeSet, RecordBatch, TraceqlError, int64_value};

pub(crate) fn insert_i64_value(
    batch: &RecordBatch,
    row: usize,
    values: &mut BTreeSet<(String, String)>,
    type_: &str,
    column: &str,
) -> Result<(), TraceqlError> {
    values.insert((
        type_.to_string(),
        int64_value(batch, column, row)?.to_string(),
    ));
    Ok(())
}
