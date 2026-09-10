use super::{PromotedSpanAttr, RecordBatch, TracesError, block_promoted_attrs};

/// The promoted attributes a merge of `batches` must carry.
///
/// Compaction inputs need not agree: an operator can edit
/// `--promote-span-attr` between two flushes, and the blocks either side of
/// that edit are compacted together. The merged schema is the union of the
/// inputs' promoted attributes, in first-seen order, so no input's dedicated
/// column is dropped.
///
/// Two inputs that promote the same key at different types have no single
/// column that can hold both, so that is an error rather than a silent choice
/// of one.
pub(crate) fn merged_promoted_attrs(
    batches: &[RecordBatch],
) -> Result<Vec<PromotedSpanAttr>, TracesError> {
    let mut merged: Vec<PromotedSpanAttr> = Vec::new();
    for batch in batches {
        for attr in block_promoted_attrs(&batch.schema())? {
            match merged.iter().find(|seen| seen.key == attr.key) {
                Some(seen) if seen.value_type != attr.value_type => {
                    return Err(TracesError::Block(format!(
                        "promoted attribute `{}` is {:?} in one input block and {:?} in \
                         another; recompact the inputs one configuration at a time",
                        attr.key, seen.value_type, attr.value_type
                    )));
                }
                Some(_) => {}
                None => merged.push(attr),
            }
        }
    }
    Ok(merged)
}
