use super::{Array, RecordBatch, StringArray, TracesError};

pub(crate) fn string_column<'a>(
    batch: &'a RecordBatch,
    column: &str,
) -> Result<&'a StringArray, TracesError> {
    batch
        .column_by_name(column)
        .and_then(|col| col.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| TracesError::Block(format!("{column} is not Utf8")))
}
