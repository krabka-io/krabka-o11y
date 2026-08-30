use super::{
    Array, AttrValue, BLOCK_ATTR_KEYS, BLOCK_ATTR_VALUE, BLOCK_ATTR_VALUE_BOOL,
    BLOCK_ATTR_VALUE_DOUBLE, BLOCK_ATTR_VALUE_INT, RESOURCE_ATTR_PREFIX, RecordBatch, Result,
    StringArray, TraceqlError, block_attr_values_for_key, optional_list_column,
};

pub(crate) fn block_row_attrs(batch: &RecordBatch, row: usize) -> Result<Vec<(String, AttrValue)>> {
    let Some(keys) = optional_list_column(batch, BLOCK_ATTR_KEYS)? else {
        return Ok(Vec::new());
    };
    if keys.is_null(row) {
        return Ok(Vec::new());
    }
    let key_values = keys.value(row);
    let key_values = key_values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TraceqlError::Exec("attr_keys row is not Utf8".into()))?;
    let str_values = optional_list_column(batch, BLOCK_ATTR_VALUE)?;
    let int_values = optional_list_column(batch, BLOCK_ATTR_VALUE_INT)?;
    let double_values = optional_list_column(batch, BLOCK_ATTR_VALUE_DOUBLE)?;
    let bool_values = optional_list_column(batch, BLOCK_ATTR_VALUE_BOOL)?;

    let mut out = Vec::new();
    for attr_idx in 0..key_values.len() {
        if key_values.is_null(attr_idx) {
            continue;
        }
        let key = key_values.value(attr_idx);
        if key.starts_with(RESOURCE_ATTR_PREFIX) {
            continue;
        }
        out.extend(
            block_attr_values_for_key(
                str_values,
                int_values,
                double_values,
                bool_values,
                row,
                attr_idx,
            )?
            .into_iter()
            .map(|value| (key.to_string(), value)),
        );
    }
    Ok(out)
}
