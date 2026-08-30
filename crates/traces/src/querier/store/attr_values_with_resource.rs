use super::{RecordBatch, AttrValue, TraceqlError, BTreeSet, ATTR_PREFIX, Array, DataType, string_array_value, int64_array_value, float64_array_value, bool_array_value, block_attr_values};

pub(crate) fn attr_values_with_resource(
    batch: &RecordBatch,
    row: usize,
    include_resource: bool,
) -> Result<Vec<(String, AttrValue)>, TraceqlError> {
    let mut out = Vec::new();
    let mut promoted_keys = BTreeSet::new();
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let Some(key) = field.name().strip_prefix(ATTR_PREFIX) else {
            continue;
        };
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
        promoted_keys.insert(key.to_string());
        out.push((key.to_string(), value));
    }
    out.extend(block_attr_values(
        batch,
        row,
        include_resource,
        &promoted_keys,
    )?);
    Ok(out)
}
