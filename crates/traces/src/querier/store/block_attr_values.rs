use super::{
    Array, AttrValue, BTreeSet, RESOURCE_ATTR_PREFIX, RecordBatch, SCOL_ATTR_KEYS, SCOL_ATTR_VALUE,
    SCOL_ATTR_VALUE_BOOL, SCOL_ATTR_VALUE_DOUBLE, SCOL_ATTR_VALUE_INT, StringArray, TraceqlError,
    block_attr_values_for_key, optional_list_column,
};

pub(crate) fn block_attr_values(
    batch: &RecordBatch,
    row: usize,
    include_resource: bool,
    promoted_keys: &BTreeSet<String>,
) -> Result<Vec<(String, AttrValue)>, TraceqlError> {
    let Some(keys) = optional_list_column(batch, SCOL_ATTR_KEYS)? else {
        return Ok(Vec::new());
    };
    if keys.is_null(row) {
        return Ok(Vec::new());
    }
    let key_values = keys.value(row);
    let key_values = key_values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TraceqlError::Store("attr_keys row is not Utf8".into()))?;
    let str_values = optional_list_column(batch, SCOL_ATTR_VALUE)?;
    let int_values = optional_list_column(batch, SCOL_ATTR_VALUE_INT)?;
    let double_values = optional_list_column(batch, SCOL_ATTR_VALUE_DOUBLE)?;
    let bool_values = optional_list_column(batch, SCOL_ATTR_VALUE_BOOL)?;

    let mut out = Vec::new();
    for attr_idx in 0..key_values.len() {
        if key_values.is_null(attr_idx) {
            continue;
        }
        let values = block_attr_values_for_key(
            str_values,
            int_values,
            double_values,
            bool_values,
            row,
            attr_idx,
        )?;
        out.extend(values.into_iter().filter_map(|value| {
            let key = key_values.value(attr_idx);
            ((include_resource || !key.starts_with(RESOURCE_ATTR_PREFIX))
                && !promoted_keys.contains(key))
            .then(|| (key.to_string(), value))
        }));
    }
    Ok(out)
}
