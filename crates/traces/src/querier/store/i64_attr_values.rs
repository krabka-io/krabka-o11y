use super::{ListArray, TraceqlError, row_attr_values, Array, Int64Array};

pub(crate) fn i64_attr_values(
    values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
    name: &str,
) -> Result<Vec<i64>, TraceqlError> {
    let Some(values) = row_attr_values(values, row, attr_idx, name)? else {
        return Ok(Vec::new());
    };
    let values = values
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| TraceqlError::Store(format!("attribute column `{name}` is not Int64")))?;
    Ok((0..values.len())
        .filter(|idx| !values.is_null(*idx))
        .map(|idx| values.value(idx))
        .collect())
}
