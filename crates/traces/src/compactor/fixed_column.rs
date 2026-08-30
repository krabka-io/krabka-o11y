use super::{Array, FixedSizeBinaryArray, RecordBatch, TracesError};

pub(crate) fn fixed_column<'a>(
    batch: &'a RecordBatch,
    column: &str,
    width: i32,
) -> Result<&'a FixedSizeBinaryArray, TracesError> {
    let array = batch
        .column_by_name(column)
        .ok_or_else(|| TracesError::Block(format!("missing column {column}")))?
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| TracesError::Block(format!("{column} is not FixedSizeBinary")))?;
    if array.value_length() != width {
        return Err(TracesError::Block(format!(
            "{column} is FixedSizeBinary({}), expected {width}",
            array.value_length()
        )));
    }
    Ok(array)
}
