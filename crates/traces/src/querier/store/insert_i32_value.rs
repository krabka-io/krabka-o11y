use super::{RecordBatch, BTreeSet, TraceqlError, int32_value};

pub(crate) fn insert_i32_value(
    batch: &RecordBatch,
    row: usize,
    values: &mut BTreeSet<(String, String)>,
    column: &str,
) -> Result<(), TraceqlError> {
    values.insert((
        "int".to_string(),
        int32_value(batch, column, row)?.to_string(),
    ));
    Ok(())
}
