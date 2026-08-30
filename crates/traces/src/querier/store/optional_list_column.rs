use super::{RecordBatch, ListArray, TraceqlError, Array};

pub(crate) fn optional_list_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<Option<&'a ListArray>, TraceqlError> {
    batch
        .column_by_name(name)
        .map(|col| {
            col.as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| TraceqlError::Store(format!("nested column `{name}` is not a list")))
        })
        .transpose()
}
