use super::*;

#[tokio::test]
pub(crate) async fn compaction_frontier_refresh_treats_absent_manifest_as_empty() {
    let store = object_store::memory::InMemory::new();
    let prefix = ObjectPath::default();
    let frontier = SharedCompactionFrontier::new(CompactionFrontier::new(123));
    let hot_tail = BufferedLogHotTail::default();
    let fresh = hot_tail_test_record(3_000, "new");
    hot_tail.append_records(vec![fresh.clone()]);

    let pruned = refresh_compaction_frontier_and_prune(&store, &prefix, &frontier, &hot_tail)
        .await
        .unwrap();

    assert_eq!(pruned, 0);
    assert_eq!(frontier.snapshot(), CompactionFrontier::new(123));
    assert_eq!(hot_tail.records(), vec![fresh]);
}
