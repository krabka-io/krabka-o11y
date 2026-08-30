use super::{StructArray, Int64Array, TraceqlError, Array};

pub(crate) fn struct_int64_field<'a>(
    values: &'a StructArray,
    field: usize,
    name: &str,
) -> Result<&'a Int64Array, TraceqlError> {
    values
        .columns()
        .get(field)
        .and_then(|col| col.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| TraceqlError::Store(format!("nested column `{name}` missing int64 field")))
}
