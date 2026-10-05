use krabka_blockstore::{BlockDescriptor, BlockKey, LabelIndex, LogBlockIndex, TimeRange, labels};

#[test]
fn consuming_indexes_preserve_order_and_string_buffers() {
    let mut index = LabelIndex::default();
    let fp = index.insert_series("tenant", labels([("app", "api"), ("é", "🦀")]));
    index.insert_series("other", labels([("app", "hidden")]));
    let buffer = index.labels_for("tenant", fp).unwrap()["app"].as_ptr();
    let owned = index.into_tenant_series("tenant").collect::<Vec<_>>();
    assert2::assert!(owned == vec![(fp, labels([("app", "api"), ("é", "🦀")]))]);
    assert2::assert!(owned[0].1["app"].as_ptr() == buffer);
    assert2::assert!(LabelIndex::default().into_tenant_series("missing").count() == 0);

    let mut blocks = LogBlockIndex::default();
    for offset in [3, 1] {
        blocks.insert(BlockDescriptor::new(
            BlockKey::new("tenant", 0, offset, offset, TimeRange::new(10, 20).unwrap()),
            [fp].into(),
        ));
    }
    let tenant_buffer = blocks.blocks()[0].key.tenant.as_ptr();
    let owned = blocks.into_blocks();
    let expected = [1, 3].map(|offset| {
        BlockDescriptor::new(
            BlockKey::new("tenant", 0, offset, offset, TimeRange::new(10, 20).unwrap()),
            [fp].into(),
        )
    });
    assert2::assert!(owned == expected);
    assert2::assert!(owned[0].key.tenant.as_ptr() == tenant_buffer);
    assert2::assert!(LogBlockIndex::default().into_blocks().is_empty());
}
