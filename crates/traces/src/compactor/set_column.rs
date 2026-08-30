use super::{ArrayRef, TracesError};

pub(crate) fn set_column(
    schema: &arrow::datatypes::SchemaRef,
    columns: &mut [ArrayRef],
    name: &str,
    array: ArrayRef,
) -> Result<(), TracesError> {
    let idx = schema
        .column_with_name(name)
        .ok_or_else(|| TracesError::Block(format!("missing column {name}")))?
        .0;
    columns[idx] = array;
    Ok(())
}
