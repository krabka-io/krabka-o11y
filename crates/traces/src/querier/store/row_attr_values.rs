use super::{ListArray, TraceqlError, Array};

pub(crate) fn row_attr_values(
    values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
    name: &str,
) -> Result<Option<arrow::array::ArrayRef>, TraceqlError> {
    let Some(values) = values else {
        return Ok(None);
    };
    if values.is_null(row) {
        return Ok(None);
    }
    let row_values = values.value(row);
    let row_values = row_values
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            TraceqlError::Store(format!("attribute column `{name}` row is not a list"))
        })?;
    if attr_idx >= row_values.len() || row_values.is_null(attr_idx) {
        return Ok(None);
    }
    Ok(Some(row_values.value(attr_idx)))
}
