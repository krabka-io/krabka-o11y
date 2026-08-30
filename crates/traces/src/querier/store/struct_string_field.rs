use super::{Array, StructArray, TraceqlError};

pub(crate) fn struct_string_field<'a>(
    values: &'a StructArray,
    field: usize,
    name: &str,
) -> Result<&'a dyn Array, TraceqlError> {
    values
        .columns()
        .get(field)
        .map(std::convert::AsRef::as_ref)
        .ok_or_else(|| TraceqlError::Store(format!("nested column `{name}` missing string field")))
}
