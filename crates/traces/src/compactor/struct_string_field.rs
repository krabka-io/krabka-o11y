use super::*;

pub(crate) fn struct_string_field(array: &StructArray, idx: usize) -> Result<&StringArray, TracesError> {
    array
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TracesError::Block(format!("struct field {idx} is not Utf8")))
}
