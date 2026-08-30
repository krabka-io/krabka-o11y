use super::*;

pub(crate) fn struct_fixed_field<'a>(
    values: &'a StructArray,
    field: usize,
    name: &str,
) -> Result<&'a FixedSizeBinaryArray, TraceqlError> {
    values
        .columns()
        .get(field)
        .and_then(|col| col.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| {
            TraceqlError::Store(format!("nested column `{name}` missing fixed binary field"))
        })
}
