use super::*;

pub(crate) fn struct_list_field(array: &StructArray, idx: usize) -> Result<&ListArray, TracesError> {
    array
        .column(idx)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| TracesError::Block(format!("struct field {idx} is not a list")))
}
