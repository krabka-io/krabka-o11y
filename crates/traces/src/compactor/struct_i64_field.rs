use super::*;

pub(crate) fn struct_i64_field(array: &StructArray, idx: usize) -> Result<&Int64Array, TracesError> {
    array
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| TracesError::Block(format!("struct field {idx} is not Int64")))
}
