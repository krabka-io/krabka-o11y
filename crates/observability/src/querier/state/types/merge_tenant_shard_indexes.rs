use super::*;

pub(crate) fn merge_tenant_shard_indexes(
    tenant: &str,
    indexes: impl IntoIterator<Item = (LabelIndex, BlockIndex)>,
) -> (LabelIndex, BlockIndex) {
    let mut merged_labels = LabelIndex::default();
    let mut merged_blocks = BTreeMap::new();

    for (label_index, block_index) in indexes {
        for (_, labels) in label_index.tenant_series(tenant) {
            merged_labels.insert_series(tenant.to_string(), labels);
        }
        for block in block_index.blocks() {
            merged_blocks
                .entry(block.key.object_key())
                .or_insert_with(|| block.clone());
        }
    }

    let mut block_index = BlockIndex::default();
    for block in merged_blocks.into_values() {
        block_index.insert(block);
    }

    (merged_labels, block_index)
}
