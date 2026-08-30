use super::{RecordBatch, Int64Array, TracesError, Array};

pub(crate) fn int64_column<'a>(batch: &'a RecordBatch, column: &str) -> Result<&'a Int64Array, TracesError> {
    batch
        .column_by_name(column)
        .and_then(|col| col.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| TracesError::Block(format!("{column} is not Int64")))
}
