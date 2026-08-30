use super::{Array, FixedSizeBinaryArray, StructArray, TracesError};

pub(crate) fn struct_fixed_field(
    array: &StructArray,
    idx: usize,
) -> Result<&FixedSizeBinaryArray, TracesError> {
    array
        .column(idx)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| TracesError::Block(format!("struct field {idx} is not FixedSizeBinary")))
}
