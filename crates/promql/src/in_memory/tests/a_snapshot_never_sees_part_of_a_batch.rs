use super::*;

/// A query holds its snapshot for the whole scan, so a write that lands during
/// one must not reach into it. This pins both ends of that: a snapshot taken
/// before a batch sees the head as it was before the batch, and one taken after
/// sees every record in it. The middle is what a batched write is most likely
/// to lose -- a reader that catches a prefix -- and there is no observation
/// point in this test that produces one.
#[tokio::test]
pub(crate) async fn a_snapshot_never_sees_part_of_a_batch() {
    let head = WalHead::new();
    let series = lbls(&[("__name__", "up"), ("job", "api")]);
    head.apply_wal_record(&float_record("t", &series, 1_000, 1.0));

    let before = head.snapshot();

    let batch = [
        (
            float_record("t", &series, 2_000, 2.0),
            PartitionIndex(0),
            Offset(7),
        ),
        (
            float_record("t", &series, 3_000, 3.0),
            PartitionIndex(0),
            Offset(8),
        ),
    ];
    head.apply_wal_records_at(
        batch
            .iter()
            .map(|(record, partition, offset)| (record, *partition, *offset)),
    );

    let after = head.snapshot();

    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let count = async |store: &InMemoryMetricStore| {
        let scan = store
            .scan("t", &matchers, i64::MIN, i64::MAX)
            .await
            .unwrap();
        let table = scan.float_table.clone().unwrap();
        count_rows(&scan, &table).await
    };

    // The pre-batch snapshot is frozen at the one record it captured.
    assert2::assert!(count(&before).await == 1);
    // The post-batch snapshot has both of the batch's records, not one.
    assert2::assert!(count(&after).await == 3);
    // And the live head agrees with the snapshot taken from it.
    let live = head.scan("t", &matchers, i64::MIN, i64::MAX).await.unwrap();
    let table = live.float_table.clone().unwrap();
    assert2::assert!(count_rows(&live, &table).await == 3);

    // The batch carried its offsets through with it.
    assert2::assert!(head.high_water_offset(PartitionIndex(0)) == Some(Offset(8)));
    assert2::assert!(head.low_water_offset(PartitionIndex(0)) == Some(Offset(7)));
}
