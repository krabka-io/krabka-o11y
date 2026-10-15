use super::*;

mod query_shard_matcher_filters_by_series_fingerprint_modulo {
use super::*;

#[tokio::test]
pub(crate) async fn query_shard_matcher_filters_by_series_fingerprint_modulo() {
    assert_query_shard_selects(MatchOp::Eq).await;
}

}

mod query_shard_neq_matcher_excludes_matching_fingerprint_modulo {
use super::*;

#[tokio::test]
pub(crate) async fn query_shard_neq_matcher_excludes_matching_fingerprint_modulo() {
    assert_query_shard_selects(MatchOp::Neq).await;
}

}


mod offsets_track_low_and_high_water {
use super::*;
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
}


mod regex_matchers_are_anchored_and_absent_labels_match_empty {
use super::*;
use super::*;

#[tokio::test]
pub(crate) async fn regex_matchers_are_anchored_and_absent_labels_match_empty() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "t",
        lbls(&[
            crate::test_support::TestLabelPair::new("__name__", "up"),
            crate::test_support::TestLabelPair::new("job", "api"),
        ]),
        1,
        1.0,
    );
    store.push_float(
        "t",
        lbls(&[crate::test_support::TestLabelPair::new("__name__", "up")]),
        1,
        2.0,
    );

    let anchored = [LabelMatcher::new("job", MatchOp::Re, "a")];
    assert2::assert!(
        store
            .scan("t", &anchored, 0, 10)
            .await
            .unwrap()
            .float_table
            .is_none()
    );

    let empty = [LabelMatcher::new("missing", MatchOp::Eq, "")];
    assert2::assert!(
        store
            .scan("t", &empty, 0, 10)
            .await
            .unwrap()
            .float_table
            .is_some()
    );
}
}


mod store_cardinality_and_tsdb_stats_include_float_and_hist_series {
use super::*;
use super::*;

#[tokio::test]
pub(crate) async fn store_cardinality_and_tsdb_stats_include_float_and_hist_series() {
    let fixture = store_with_float_and_hist_series();
    let store = fixture.store;

    check!(
        store.cardinality_label_names("tenant-a").await.unwrap()
            == expected_label_name_cardinality()
    );
    check!(
        store.cardinality_label_values("tenant-a").await.unwrap()
            == expected_label_value_cardinality()
    );

    let stats = store.tsdb_stats("tenant-a").await.unwrap();
    assert2::assert!(
        stats.head_stats
            == TsdbHeadStats {
                num_series: 3,
                num_samples: 3,
                num_chunks: 3,
                min_time: 1_000,
                max_time: 3_000,
            }
    );
    assert2::assert!(stats.label_value_count_by_label_name == expected_label_value_count_stats());
    assert2::assert!(stats.memory_in_bytes_by_label_name == expected_label_memory_stats());
    assert2::assert!(stats.series_count_by_label_value_pair == expected_label_pair_stats());
}
}
