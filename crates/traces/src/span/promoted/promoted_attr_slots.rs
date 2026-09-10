use super::{
    ListArray, RecordBatch, SCOL_ATTR_IS_ARRAY, SCOL_ATTR_KEYS, TracesError, row_attr_slot,
};

/// Where `key` sits in each row's generic attribute list.
///
/// One entry per row of `batch`, so a caller can walk the rows once and read
/// the value out of whichever typed value column its declared type names.
pub(crate) fn promoted_attr_slots(
    batch: &RecordBatch,
    key: &str,
) -> Result<Vec<Option<usize>>, TracesError> {
    let keys = batch
        .column_by_name(SCOL_ATTR_KEYS)
        .and_then(|column| column.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| TracesError::Block(format!("{SCOL_ATTR_KEYS} is not a list")))?;
    let is_array = batch
        .column_by_name(SCOL_ATTR_IS_ARRAY)
        .and_then(|column| column.as_any().downcast_ref::<ListArray>());

    (0..batch.num_rows())
        .map(|row| row_attr_slot(keys, is_array, row, key))
        .collect()
}
