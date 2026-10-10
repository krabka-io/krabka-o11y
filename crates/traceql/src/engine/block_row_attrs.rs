use super::{AttrValue, RESOURCE_ATTR_PREFIX, RecordBatch, Result, block_row_attrs_where};

pub(crate) fn block_row_attrs(batch: &RecordBatch, row: usize) -> Result<Vec<(String, AttrValue)>> {
    block_row_attrs_where(batch, row, |key| !key.starts_with(RESOURCE_ATTR_PREFIX))
}
