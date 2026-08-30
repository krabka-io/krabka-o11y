use super::*;

#[test]
pub(crate) fn hot_tail_prune_compacted_rebuilds_records_and_time_index() {
    let bucket = minutes(1).nanos_i64();

    let hot_tail = BufferedLogHotTail::default();
    let mut compacted_by_offset = hot_tail_test_record(4 * bucket, "offset-old");
    compacted_by_offset.position = Some(WalPosition {
        partition: PartitionIndex(0),
        offset: Offset(7),
    });
    let mut kept_by_offset = hot_tail_test_record(3 * bucket, "offset-new");
    kept_by_offset.position = Some(WalPosition {
        partition: PartitionIndex(0),
        offset: Offset(8),
    });
    let compacted_by_time = hot_tail_test_record(2 * bucket, "time-old");
    let kept_by_time = hot_tail_test_record(5 * bucket, "time-new");
    let expected = vec![kept_by_offset.clone(), kept_by_time.clone()];

    hot_tail.append_records(vec![
        compacted_by_offset,
        kept_by_offset,
        compacted_by_time,
        kept_by_time,
    ]);

    let frontier =
        CompactionFrontier::new(2 * bucket).with_partition_offset(PartitionIndex(0), Offset(7));

    assert_eq!(hot_tail.prune_compacted(&frontier), 2);
    assert_eq!(hot_tail.records(), expected);
    assert2::assert!(hot_tail.records_in_range(0, 6 * bucket) == expected);
    assert!(hot_tail.records_in_range(2 * bucket, 2 * bucket).is_empty());
    assert!(hot_tail.records_in_range(4 * bucket, 4 * bucket).is_empty());
}
