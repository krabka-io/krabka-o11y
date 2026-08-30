use super::{Array, RecordBatch, string_array_value};

pub(crate) fn string_value(batch: &RecordBatch, col: &str, row: usize) -> Option<String> {
    let arr = batch.column_by_name(col)?;
    if arr.is_null(row) {
        return None;
    }
    string_array_value(arr.as_ref(), row)
}
