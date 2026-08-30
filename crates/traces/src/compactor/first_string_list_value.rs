use super::*;

pub(crate) fn first_string_list_value<A: MetadataValueArray + 'static>(
    batch: &RecordBatch,
    column: &str,
    row: usize,
    attr_idx: usize,
) -> Result<Option<String>, TracesError> {
    let values = list_column(batch, column)?;
    if values.is_null(row) {
        return Ok(None);
    }
    let row_values = values.value(row);
    let row_values = row_values
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| TracesError::Block(format!("{column} row is not a list")))?;
    if attr_idx >= row_values.len() || row_values.is_null(attr_idx) {
        return Ok(None);
    }
    let attr_values = row_values.value(attr_idx);
    let attr_values = attr_values
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| TracesError::Block(format!("{column} values have wrong type")))?;
    for value_idx in 0..attr_values.len() {
        if !attr_values.is_null(value_idx) {
            return Ok(Some(attr_values.string_value(value_idx)));
        }
    }
    Ok(None)
}
