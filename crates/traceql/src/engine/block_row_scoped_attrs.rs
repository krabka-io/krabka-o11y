use super::{AttrValue, RecordBatch, Result, block_row_attrs_where};

/// Like `block_row_attrs`, but keeps the `__resource.`-prefixed keys.
///
/// The caller can then split the span scope from the resource scope. The
/// search path drops the resource attributes.
pub(crate) fn block_row_scoped_attrs(
    batch: &RecordBatch,
    row: usize,
) -> Result<Vec<(String, AttrValue)>> {
    block_row_attrs_where(batch, row, |_| true)
}
