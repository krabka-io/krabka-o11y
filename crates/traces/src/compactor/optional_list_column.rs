use super::*;

pub(crate) fn optional_list_column<'a>(
    batch: &'a RecordBatch,
    column: &str,
) -> Result<Option<&'a ListArray>, TracesError> {
    let Some(col) = batch.column_by_name(column) else {
        return Ok(None);
    };
    col.as_any()
        .downcast_ref::<ListArray>()
        .map(Some)
        .ok_or_else(|| TracesError::Block(format!("{column} is not a list")))
}
