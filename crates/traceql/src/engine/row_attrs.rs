use super::{
    ATTR_PREFIX, Array, AsArray, AttrValue, DataType, RecordBatch, Result, TraceqlError,
    block_row_attrs, string_array_value,
};

pub(crate) fn row_attrs(batch: &RecordBatch, row: usize) -> Result<Vec<(String, AttrValue)>> {
    let schema = batch.schema();
    let mut attrs = Vec::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        let Some(name) = field.name().strip_prefix(ATTR_PREFIX) else {
            continue;
        };
        let array = batch.column(idx);
        if array.is_null(row) {
            continue;
        }
        let value = match field.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                AttrValue::Str(string_array_value(array.as_ref(), row).ok_or_else(|| {
                    TraceqlError::Exec(format!("unsupported string attribute column {name}"))
                })?)
            }
            DataType::Int64 => AttrValue::Int(
                array
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .value(row),
            ),
            DataType::Float64 => AttrValue::Float(
                array
                    .as_primitive::<arrow::datatypes::Float64Type>()
                    .value(row),
            ),
            DataType::Boolean => AttrValue::Bool(array.as_boolean().value(row)),
            other => {
                return Err(TraceqlError::Exec(format!(
                    "unsupported attribute column type {other:?}"
                )));
            }
        };
        attrs.push((name.to_string(), value));
    }
    attrs.extend(block_row_attrs(batch, row)?);
    attrs.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(attrs)
}
