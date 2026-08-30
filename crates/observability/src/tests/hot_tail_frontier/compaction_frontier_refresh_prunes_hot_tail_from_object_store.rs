use super::*;

#[tokio::test]
pub(crate) async fn compaction_frontier_refresh_prunes_hot_tail_from_object_store() {
    let store = object_store::memory::InMemory::new();
    let prefix = ObjectPath::default();
    let frontier = SharedCompactionFrontier::default();
    let hot_tail = BufferedLogHotTail::default();
    let compacted = hot_tail_test_record(1_000, "old");
    let fresh = hot_tail_test_record(3_000, "new");
    hot_tail.append_records(vec![compacted, fresh.clone()]);
    write_compaction_frontier_to_object_store(&store, &prefix, &CompactionFrontier::new(2_000))
        .await
        .unwrap();

    let pruned = refresh_compaction_frontier_and_prune(&store, &prefix, &frontier, &hot_tail)
        .await
        .unwrap();

    assert_eq!(pruned, 1);
    assert_eq!(frontier.snapshot(), CompactionFrontier::new(2_000));
    assert_eq!(hot_tail.records(), vec![fresh]);
}
