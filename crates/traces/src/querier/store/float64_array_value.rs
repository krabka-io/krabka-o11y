use super::{Array, Float64Array, TraceqlError};

pub(crate) fn float64_array_value(col: &dyn Array, row: usize) -> Result<f64, TraceqlError> {
    col.as_any()
        .downcast_ref::<Float64Array>()
        .map(|a| a.value(row))
        .ok_or_else(|| TraceqlError::Store("unsupported float64 column type".into()))
}
