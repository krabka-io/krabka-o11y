use super::{RecordBatch, TraceqlError, int64_array_value};

pub(crate) fn int64_value(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<i64, TraceqlError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| TraceqlError::Store(format!("missing int64 column `{name}`")))?;
    int64_array_value(col.as_ref(), row)
}
