use super::{RecordBatch, TraceqlError, Array, string_array_value};

pub(crate) fn string_value(batch: &RecordBatch, name: &str, row: usize) -> Result<String, TraceqlError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| TraceqlError::Store(format!("missing string column `{name}`")))?;
    if col.is_null(row) {
        return Ok(String::new());
    }
    string_array_value(col.as_ref(), row)
}
