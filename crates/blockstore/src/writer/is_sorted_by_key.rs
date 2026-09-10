use super::{RecordBatch, Result, SortKeyCheck};

/// Whether `batches`, read end to end, are already ordered by `sort_key`.
///
/// One comparison pass and no allocation per row, so the cost on the path a
/// correct caller takes is a scan of the key columns rather than a sort. The
/// relation itself lives in [`SortKeyCheck`], which is also what the streaming
/// writer enforces, so the buffered and streaming paths cannot disagree about
/// what "in order" means.
///
/// # Errors
/// Returns [`BlockStoreError::InvalidBlock`](crate::BlockStoreError::InvalidBlock)
/// when a batch lacks a declared sort-key column.
pub(crate) fn is_sorted_by_key(batches: &[RecordBatch], sort_key: &[String]) -> Result<bool> {
    let mut check = SortKeyCheck::new(sort_key);
    for batch in batches {
        if !check.accept(batch)? {
            return Ok(false);
        }
    }
    Ok(true)
}
