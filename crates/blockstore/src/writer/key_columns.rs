use super::{ArrayRef, BlockStoreError, RecordBatch, Result};

/// The sort-key columns of `batch`, in declaration order.
pub(crate) fn key_columns(batch: &RecordBatch, sort_key: &[String]) -> Result<Vec<ArrayRef>> {
    sort_key
        .iter()
        .map(|name| {
            batch.column_by_name(name).cloned().ok_or_else(|| {
                BlockStoreError::InvalidBlock(format!("missing sort-key column `{name}`"))
            })
        })
        .collect()
}
