use super::{Array, TraceqlError, BooleanArray};

pub(crate) fn bool_array_value(col: &dyn Array, row: usize) -> Result<bool, TraceqlError> {
    col.as_any()
        .downcast_ref::<BooleanArray>()
        .map(|a| a.value(row))
        .ok_or_else(|| TraceqlError::Store("unsupported bool column type".into()))
}
