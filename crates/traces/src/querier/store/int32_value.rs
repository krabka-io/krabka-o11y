use super::{Array, Int32Array, RecordBatch, TraceqlError};

pub(crate) fn int32_value(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<i32, TraceqlError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| TraceqlError::Store(format!("missing int32 column `{name}`")))?;
    col.as_any()
        .downcast_ref::<Int32Array>()
        .map(|a| a.value(row))
        .ok_or_else(|| TraceqlError::Store("unsupported int32 column type".into()))
}
