use super::*;

#[derive(bon::Builder)]
struct OffsetExpectation {
    partition: i32,
    high: Option<i64>,
    low: Option<i64>,
}

#[test]
pub(crate) fn offsets_track_low_and_high_water() {
    let head = WalHead::new();
    let record = |ts: i64| WalRecord {
        tenant: "t".to_string(),
        labels: vec![krabka_metrics::MetricLabelPair::new(
            "__name__",
            "up".to_string().into(),
        )],
        payload: SamplePayload::Float {
            timestamp_ms: ts,
            value: 1.0,
            start_timestamp_ms: None,
        },
        exemplars: Vec::new(),
    };

    // No offsets ingested yet.
    assert2::assert!(head.high_water_offset(PartitionIndex(0)) == None);
    assert2::assert!(head.low_water_offset(PartitionIndex(0)) == None);

    head.apply_wal_record_at(&record(10), PartitionIndex(0), Offset(5));
    head.apply_wal_record_at(&record(20), PartitionIndex(0), Offset(6));
    head.apply_wal_record_at(&record(30), PartitionIndex(1), Offset(100));

    // High water is the latest applied offset per partition, low water the
    // first; untracked partitions stay empty.
    for expectation in [
        OffsetExpectation::builder()
            .partition(0)
            .high(6)
            .low(5)
            .build(),
        OffsetExpectation::builder()
            .partition(1)
            .high(100)
            .low(100)
            .build(),
        OffsetExpectation::builder().partition(2).build(),
    ] {
        assert2::assert!(
            head.high_water_offset(PartitionIndex(expectation.partition))
                == expectation.high.map(Offset)
        );
        assert2::assert!(
            head.low_water_offset(PartitionIndex(expectation.partition))
                == expectation.low.map(Offset)
        );
    }

    // Pruning does not move offsets (they track ingestion, not retention).
    let _ = head.prune(i64::MAX);
    assert2::assert!(head.high_water_offset(PartitionIndex(0)) == Some(Offset(6)));
    assert2::assert!(head.low_water_offset(PartitionIndex(0)) == Some(Offset(5)));
}
