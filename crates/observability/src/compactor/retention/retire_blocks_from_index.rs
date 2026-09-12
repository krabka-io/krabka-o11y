use super::{BTreeSet, BlockIndex, CompactorRunError, LabelIndex, insert_descriptor_labels};

/// Rebuilds one index pair without the blocks in `expired`.
///
/// Returns `None` when the index names none of them, so the caller writes
/// nothing. Every log index write is a whole-object put, and a put that only
/// rewrites the bytes that are already there still races another writer.
///
/// The label index is rebuilt from the blocks that stay, not copied. A series
/// that only the retired blocks held has no rows left to match, and leaving it
/// in the postings would answer `/loki/api/v1/series` with a stream that no
/// block holds.
///
/// # Errors
/// [`CompactorRunError::MissingSeriesLabels`] when a block that stays names a
/// fingerprint that the label index does not hold.
pub(crate) fn retire_blocks_from_index(
    tenant: &str,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
    expired: &BTreeSet<String>,
) -> Result<Option<(LabelIndex, BlockIndex)>, CompactorRunError> {
    if !block_index
        .blocks()
        .iter()
        .any(|block| expired.contains(&block.key.object_key()))
    {
        return Ok(None);
    }

    let mut next_label_index = LabelIndex::default();
    let mut next_block_index = BlockIndex::default();
    for block in block_index.blocks() {
        if expired.contains(&block.key.object_key()) {
            continue;
        }
        insert_descriptor_labels(&mut next_label_index, label_index, tenant, block)?;
        next_block_index.insert(block.clone());
    }
    Ok(Some((next_label_index, next_block_index)))
}
