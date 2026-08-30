use super::{Array, ListArray, StructArray, TraceqlError};

pub(crate) fn struct_list_field<'a>(
    values: &'a StructArray,
    field: usize,
    name: &str,
) -> Result<&'a ListArray, TraceqlError> {
    values
        .columns()
        .get(field)
        .and_then(|col| col.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| TraceqlError::Store(format!("nested column `{name}` missing list field")))
}
