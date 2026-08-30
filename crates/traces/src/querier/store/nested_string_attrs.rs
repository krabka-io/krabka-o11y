use super::{Array, AttrValue, ListArray, StringArray, TraceqlError};

pub(crate) fn nested_string_attrs(
    keys: &ListArray,
    values: &ListArray,
    row: usize,
) -> Result<Vec<(String, AttrValue)>, TraceqlError> {
    if keys.is_null(row) || values.is_null(row) {
        return Ok(Vec::new());
    }
    let key_values = keys.value(row);
    let key_values = key_values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TraceqlError::Store("nested attribute keys are not strings".into()))?;
    let value_lists = values.value(row);
    let value_lists = value_lists
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            TraceqlError::Store("nested attribute values are not string lists".into())
        })?;

    let mut out = Vec::new();
    for idx in 0..key_values.len().min(value_lists.len()) {
        if key_values.is_null(idx) || value_lists.is_null(idx) {
            continue;
        }
        let scalar_values = value_lists.value(idx);
        let scalar_values = scalar_values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                TraceqlError::Store("nested attribute scalar values are not strings".into())
            })?;
        if scalar_values.is_empty() || scalar_values.is_null(0) {
            continue;
        }
        out.push((
            key_values.value(idx).to_string(),
            AttrValue::Str(scalar_values.value(0).to_string()),
        ));
    }
    Ok(out)
}
