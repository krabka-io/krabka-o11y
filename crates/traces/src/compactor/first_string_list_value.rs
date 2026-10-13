use super::{MetadataValueArray, RecordBatch, TracesError, list_column};

pub(crate) fn first_string_list_value<A: MetadataValueArray + 'static>(
    batch: &RecordBatch,
    column: &str,
    row: usize,
    attr_idx: usize,
) -> Result<Option<String>, TracesError> {
    let values = list_column(batch, column)?;
    let Some(attr_values) = crate::nested_list_value(&values, column, row, attr_idx)? else {
        return Ok(None);
    };
    let attr_values = attr_values
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| TracesError::Block(format!("{column} values have wrong type")))?;
    Ok(crate::first_non_null_array_index(attr_values)
        .map(|value_idx| attr_values.string_value(value_idx)))
}
