use super::*;

pub(crate) fn attr_value(
    batch: &RecordBatch,
    row: usize,
    attr_idx: usize,
) -> Result<Option<String>, TracesError> {
    if let Some(value) =
        first_string_list_value::<StringArray>(batch, SCOL_ATTR_VALUE, row, attr_idx)?
    {
        return Ok(Some(value));
    }
    if let Some(value) =
        first_string_list_value::<Int64Array>(batch, SCOL_ATTR_VALUE_INT, row, attr_idx)?
    {
        return Ok(Some(value));
    }
    if let Some(value) =
        first_string_list_value::<Float64Array>(batch, SCOL_ATTR_VALUE_DOUBLE, row, attr_idx)?
    {
        return Ok(Some(value));
    }
    first_string_list_value::<BooleanArray>(batch, SCOL_ATTR_VALUE_BOOL, row, attr_idx)
}
