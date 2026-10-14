use super::{
    ATTR_PREFIX, Array, AttrValue, Attribute, DataType, RESOURCE_ATTR_PREFIX, RecordBatch,
    ResourceAttrs, TraceqlError, block_row_attrs_where, bool_array_value, float64_array_value,
    int64_array_value, string_array_value,
};

pub(crate) fn attr_values_with_resource(
    batch: &RecordBatch,
    row: usize,
    resource: ResourceAttrs,
) -> Result<Vec<Attribute>, TraceqlError> {
    let packed = block_row_attrs_where(batch, row, |key| {
        resource == ResourceAttrs::Include || !key.starts_with(RESOURCE_ATTR_PREFIX)
    })?;
    let mut out = Vec::new();
    let schema = batch.schema();
    let fields = schema.fields();
    for idx in 0..fields.len() {
        let field = &fields[idx];
        let Some(key) = field.name().strip_prefix(ATTR_PREFIX) else {
            continue;
        };
        if packed.iter().any(|attribute| attribute.key == key) {
            continue;
        }
        let col = batch.column(idx);
        if col.is_null(row) {
            continue;
        }
        let value = match field.data_type() {
            DataType::Utf8 => AttrValue::Str(string_array_value(col.as_ref(), row)?),
            DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Utf8 => {
                AttrValue::Str(string_array_value(col.as_ref(), row)?)
            }
            DataType::Int64 => AttrValue::Int(int64_array_value(col.as_ref(), row)?),
            DataType::Float64 => AttrValue::Float(float64_array_value(col.as_ref(), row)?),
            DataType::Boolean => AttrValue::Bool(bool_array_value(col.as_ref(), row)?),
            _ => continue,
        };
        out.push(Attribute::new(key, value));
    }
    out.extend(packed);
    Ok(out)
}
