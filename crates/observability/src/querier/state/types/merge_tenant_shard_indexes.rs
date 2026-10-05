use super::{BTreeMap, BlockIndex, LabelIndex};

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

#[cfg(test)]
mod tests {
    use krabka_blockstore::{BlockDescriptor, BlockKey, TimeRange, labels};
    use krabka_units::bytes;

    use super::{BlockIndex, LabelIndex, merge_tenant_shard_indexes};

    #[test]
    fn merged_shards_preserve_complete_indexes_and_first_block() {
        let mut first_labels = LabelIndex::default();
        let api = first_labels.insert_series("tenant", labels([("app", "api"), ("é", "🦀")]));
        first_labels.insert_series("tenant", labels([("app", "alpha")]));
        first_labels.insert_series("other", labels([("app", "hidden")]));

        let first = BlockDescriptor::new_with_size(
            BlockKey::new("tenant", 0, 1, 2, TimeRange::new(10, 20).unwrap()),
            [api].into(),
            bytes(7),
        );
        let mut first_blocks = BlockIndex::default();
        first_blocks.insert(first.clone());

        let mut second_labels = LabelIndex::default();
        second_labels.insert_series("tenant", labels([("app", "api"), ("é", "🦀")]));
        let worker = second_labels.insert_series("tenant", labels([("app", "worker")]));
        let later = BlockDescriptor::new(
            BlockKey::new("tenant", 0, 3, 4, TimeRange::new(30, 40).unwrap()),
            [worker].into(),
        );
        let mut second_blocks = BlockIndex::default();
        second_blocks.insert(BlockDescriptor::new_with_size(
            first.key.clone(),
            [worker].into(),
            bytes(99),
        ));
        second_blocks.insert(later.clone());

        let (actual_labels, actual_blocks) = merge_tenant_shard_indexes(
            "tenant",
            [(first_labels, first_blocks), (second_labels, second_blocks)],
        );
        let mut expected_labels = LabelIndex::default();
        expected_labels.insert_series("tenant", labels([("app", "api"), ("é", "🦀")]));
        expected_labels.insert_series("tenant", labels([("app", "alpha")]));
        expected_labels.insert_series("tenant", labels([("app", "worker")]));
        let mut expected_blocks = BlockIndex::default();
        expected_blocks.insert(first);
        expected_blocks.insert(later);

        assert2::assert!(actual_labels == expected_labels);
        assert2::assert!(actual_blocks == expected_blocks);
    }
}
