use super::{Array, ListArray, RecordBatch, TracesError};

pub(crate) fn list_column<'a>(batch: &'a RecordBatch, column: &str) -> Result<&'a ListArray, TracesError> {
    batch
        .column_by_name(column)
        .and_then(|col| col.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| TracesError::Block(format!("{column} is not a list")))
}
