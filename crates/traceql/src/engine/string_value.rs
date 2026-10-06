use super::{Array, RecordBatch, string_array_value};

pub(crate) fn string_value(batch: &RecordBatch, col: &str, row: usize) -> Option<String> {
    string_value_column(batch.column_by_name(col), row)
}

pub(super) fn string_value_column(
    column: Option<&arrow::array::ArrayRef>,
    row: usize,
) -> Option<String> {
    let arr = column?;
    if arr.is_null(row) {
        return None;
    }
    string_array_value(arr.as_ref(), row)
}
