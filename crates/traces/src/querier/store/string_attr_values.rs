use super::{Array, ListArray, StringArray, TraceqlError, row_attr_values};

pub(crate) fn string_attr_values(
    values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
    name: &str,
) -> Result<Vec<String>, TraceqlError> {
    let Some(values) = row_attr_values(values, row, attr_idx, name)? else {
        return Ok(Vec::new());
    };
    let values = values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TraceqlError::Store(format!("attribute column `{name}` is not Utf8")))?;
    Ok((0..values.len())
        .filter(|idx| !values.is_null(*idx))
        .map(|idx| values.value(idx).to_string())
        .collect())
}
