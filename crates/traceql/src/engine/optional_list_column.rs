use super::*;

pub(crate) fn optional_list_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<Option<&'a ListArray>> {
    batch
        .column_by_name(name)
        .map(|col| {
            col.as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| TraceqlError::Exec(format!("nested column `{name}` is not a list")))
        })
        .transpose()
}
