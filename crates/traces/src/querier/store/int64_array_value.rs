use super::{Array, Int64Array, TraceqlError};

pub(crate) fn int64_array_value(col: &dyn Array, row: usize) -> Result<i64, TraceqlError> {
    col.as_any()
        .downcast_ref::<Int64Array>()
        .map(|a| a.value(row))
        .ok_or_else(|| TraceqlError::Store("unsupported int64 column type".into()))
}
