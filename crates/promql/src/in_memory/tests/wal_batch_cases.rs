use super::*;


mod a_panicking_update_leaves_the_head_readable_and_writable {
use super::*;
use super::*;

/// The hot head is the one piece of shared state a query and the WAL tail both
/// touch, and it used to be written in place under a `std::sync` write lock.
/// A record that panicked part-way through applying would have left a
/// half-applied store behind a poisoned lock, and from that instant every
/// query and every later record on this querier would have panicked on the
/// `expect`. One bad record, one permanently broken role, still listening.
///
/// So: an update that panics leaves the head exactly as the last successful
/// one left it, and the head goes on serving reads and taking writes.
#[tokio::test]
pub(crate) async fn a_panicking_update_leaves_the_head_readable_and_writable() {
    let initial = head_with_initial_up_sample();
    let head = initial.head;
    let series = initial.series;

    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        head.update(|store| {
            store.apply_wal_record(&float_record("t", &series, 2_000, 2.0));
            panic!("one bad record");
        });
    }));

    // A later record still lands, rather than meeting a poisoned lock.
    head.apply_wal_record(&float_record("t", &series, 3_000, 3.0));

    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let scan = head
        .scan("t", &matchers, i64::MIN, i64::MAX)
        .await
        .expect("the head still answers a scan");
    let table = scan.float_table.clone().expect("the series is present");

    assert2::assert!(panicked.is_err());
    // Two rows, not three: the record the panic was part-way through writing
    // was never published, so nothing observes a half-applied update.
    assert2::assert!(count_rows(&scan, &table).await == 2);
}
}


mod a_snapshot_never_sees_part_of_a_batch {
use super::*;
use super::*;

/// A query holds its snapshot for the whole scan, so a write that lands during
/// one must not reach into it. This pins both ends of that: a snapshot taken
/// before a batch sees the head as it was before the batch, and one taken after
/// sees every record in it. The middle is what a batched write is most likely
/// to lose -- a reader that catches a prefix -- and there is no observation
/// point in this test that produces one.
#[tokio::test]
pub(crate) async fn a_snapshot_never_sees_part_of_a_batch() {
    let initial = head_with_initial_up_sample();
    let head = initial.head;
    let series = initial.series;

    let before = head.snapshot();

    let first = float_record("t", &series, 2_000, 2.0);
    let second = float_record("t", &series, 3_000, 3.0);
    head.apply_wal_records_at([
        WalRecordAt::new(&first, PartitionIndex(0), Offset(7)),
        WalRecordAt::new(&second, PartitionIndex(0), Offset(8)),
    ]);

    let after = head.snapshot();

    // The pre-batch snapshot is frozen at the one record it captured.
    assert2::assert!(count_up_float_rows(&before).await == 1);
    // The post-batch snapshot has both of the batch's records, not one.
    assert2::assert!(count_up_float_rows(&after).await == 3);
    // And the live head agrees with the snapshot taken from it.
    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let live = head.scan("t", &matchers, i64::MIN, i64::MAX).await.unwrap();
    let table = live.float_table.clone().unwrap();
    assert2::assert!(count_rows(&live, &table).await == 3);

    // The batch carried its offsets through with it.
    assert2::assert!(head.high_water_offset(PartitionIndex(0)) == Some(Offset(8)));
    assert2::assert!(head.low_water_offset(PartitionIndex(0)) == Some(Offset(7)));
}
}


mod bulk_wal_replay_and_retention_are_observable {
use super::*;
use super::*;

#[tokio::test]
pub(crate) async fn bulk_wal_replay_and_retention_are_observable() {
    assert2::assert!(DEFAULT_RETENTION == hours(6));

    let records = [
        float_record(
            "tenant-a",
            &lbls(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]),
            10_000,
            1.0,
        ),
        float_record(
            "tenant-a",
            &lbls(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "worker"),
            ]),
            20_000,
            2.0,
        ),
    ];

    let mut store = InMemoryMetricStore::with_retention(secs(5));
    assert2::assert!(store.retention() == secs(5));
    store.set_retention(secs(7));
    assert2::assert!(store.retention() == secs(7));
    store.apply_wal_records(&records);

    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let series = store
        .series("tenant-a", &matchers, 0, 30_000)
        .await
        .unwrap();
    assert2::assert!(series.len() == 2);

    let head = WalHead::with_retention(secs(9));
    assert2::assert!(head.retention() == secs(9));
    head.apply_wal_records(&records);
    let jobs = head
        .label_values("tenant-a", "job", &matchers, 0, 30_000)
        .await
        .unwrap();
    assert2::assert!(jobs == vec!["api".to_string(), "worker".to_string()]);

    let stats = head.prune(20_000);
    assert2::assert!(
        stats
            == PruneStats {
                samples_dropped: 1,
                series_dropped: 1,
            }
    );
    let jobs = head
        .label_values("tenant-a", "job", &matchers, 0, 30_000)
        .await
        .unwrap();
    assert2::assert!(jobs == vec!["worker".to_string()]);
}
}


mod cloned_wal_head_sees_records_replayed_through_original_handle {
use super::*;
use super::*;

#[tokio::test]
pub(crate) async fn cloned_wal_head_sees_records_replayed_through_original_handle() {
    let head = WalHead::new();
    let query_handle = head.clone();
    head.apply_wal_record(&WalRecord {
        tenant: "tenant-a".to_string(),
        labels: vec![
            krabka_metrics::MetricLabelPair::new("__name__", "up".to_string().into()),
            krabka_metrics::MetricLabelPair::new("job", "api".to_string().into()),
        ],
        payload: SamplePayload::Float {
            timestamp_ms: 10_000,
            value: 1.0,
            start_timestamp_ms: None,
        },
        exemplars: Vec::new(),
    });

    assert_up_query_value(query_handle).await;
}
}


mod replay_wal_records_populates_queryable_head {
use super::*;
use super::*;

#[tokio::test]
pub(crate) async fn replay_wal_records_populates_queryable_head() {
    let mut store = InMemoryMetricStore::new();
    let series_labels = vec![
        krabka_metrics::MetricLabelPair::new("__name__", "up".to_string().into()),
        krabka_metrics::MetricLabelPair::new("job", "api".to_string().into()),
    ];
    store.apply_wal_record(&WalRecord {
        tenant: "tenant-a".to_string(),
        labels: series_labels.clone(),
        payload: SamplePayload::Float {
            timestamp_ms: 10_000,
            value: 1.0,
            start_timestamp_ms: Some(5_000),
        },
        exemplars: Vec::new(),
    });
    store.apply_wal_record(&WalRecord {
        tenant: "tenant-a".to_string(),
        labels: vec![
            krabka_metrics::MetricLabelPair::new(
                "__name__",
                "request_duration_seconds".to_string().into(),
            ),
            krabka_metrics::MetricLabelPair::new("job", "api".to_string().into()),
        ],
        payload: SamplePayload::Hist {
            timestamp_ms: 10_000,
            hist: count_two_sum_three_histogram(),
        },
        exemplars: Vec::new(),
    });
    store.apply_wal_record(&WalRecord {
        tenant: "tenant-a".to_string(),
        labels: series_labels.clone(),
        payload: SamplePayload::Metadata {
            metric_family_name: "up".to_string(),
            metric_type: "gauge".to_string(),
            help: "Target health.".to_string(),
            unit: String::new(),
        },
        exemplars: Vec::new(),
    });
    store.apply_wal_record(&WalRecord {
        tenant: "tenant-a".to_string(),
        labels: series_labels.clone(),
        payload: SamplePayload::Exemplars,
        exemplars: vec![WalExemplar {
            labels: vec![krabka_metrics::MetricLabelPair::new(
                "trace_id",
                "abc".to_string(),
            )],
            value: 1.0,
            timestamp_ms: 10_000,
        }],
    });

    assert_up_query_value(store.clone()).await;
    let scan = store.scan("tenant-a", &[], 0, 10_000).await.unwrap();
    let float_table = scan.float_table.as_deref().expect("float table");
    let batches = scan
        .ctx
        .sql(&format!("SELECT start_timestamp_ms FROM {float_table}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let starts = batches[0].column(0).as_primitive::<Int64Type>();
    check!(starts.value(0) == 5_000);
    check!(
        store
            .metadata("tenant-a", Some("up"))
            .await
            .unwrap()
            .metadata[0]
            .help
            == "Target health."
    );
    check!(
        store
            .exemplars("tenant-a", &[], 0, 10_000)
            .await
            .unwrap()
            .exemplars[0]
            .labels
            == lbls(&[crate::test_support::TestLabelPair::new("trace_id", "abc")])
    );
    check!(
        store
            .scan("tenant-a", &[], 0, 10_000)
            .await
            .unwrap()
            .histogram_table
            .is_some()
    );
}
}


mod wal_head_delegates_metadata_cardinality_stats_and_blocks {
use super::*;
use super::*;

#[tokio::test]
pub(crate) async fn wal_head_delegates_metadata_cardinality_stats_and_blocks() {
    let fixture = store_with_float_and_hist_series();
    let mut store = fixture.store;
    let up_api = fixture.up_api;
    store.set_retention(millis(12_345));
    store.push_exemplar(
        "tenant-a",
        up_api.clone(),
        lbls(&[crate::test_support::TestLabelPair::new("trace_id", "abc")]),
        1_500,
        1.5,
    );
    store.push_metadata("tenant-a", "up", "gauge", "Target health.", "");
    store.push_tsdb_block("tenant-a", "block-a", 0, 5_000, 3, 3);
    store.record_offset(PartitionIndex(0), Offset(7));
    store.record_offset(PartitionIndex(0), Offset(9));

    let head = WalHead::from_store(store);
    check!(head.retention() == millis(12_345));
    check!(
        head.watermarks().get(&PartitionIndex(0))
            == Some(&PartitionWatermark {
                low_water_offset: Offset(7),
                high_water_offset: Offset(9),
            })
    );

    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let names = head
        .label_names("tenant-a", &matchers, 0, 5_000)
        .await
        .unwrap();
    check!(names == vec!["__name__".to_string(), "job".to_string()]);
    let jobs = head
        .label_values("tenant-a", "job", &matchers, 0, 5_000)
        .await
        .unwrap();
    check!(jobs == vec!["api".to_string(), "worker".to_string()]);
    check!(
        head.exemplars("tenant-a", &matchers, 0, 5_000)
            .await
            .unwrap()
            .exemplars[0]
            .labels
            == lbls(&[crate::test_support::TestLabelPair::new("trace_id", "abc")])
    );
    check!(
        head.metadata("tenant-a", Some("up"))
            .await
            .unwrap()
            .metadata[0]
            .help
            == "Target health."
    );
    check!(
        head.cardinality_active_series("tenant-a")
            .await
            .unwrap()
            .len()
            == 3
    );
    check!(
        head.cardinality_label_names("tenant-a").await.unwrap()
            == expected_label_name_cardinality()
    );
    check!(
        head.cardinality_label_values("tenant-a").await.unwrap()
            == expected_label_value_cardinality()
    );
    let stats = head.tsdb_stats("tenant-a").await.unwrap();
    assert2::assert!(stats.head_stats.num_series == 3);
    assert2::assert!(stats.head_stats.num_samples == 3);
    assert2::assert!(stats.series_count_by_metric_name == expected_metric_name_stats());
    check!(head.tsdb_blocks("tenant-a").await.unwrap()[0].id == "block-a");
}
}


mod wal_head_delete_tenant {
use super::*;
use super::{MetricStore, WalHead, lbls};

#[tokio::test]
async fn wal_head_delete_tenant_removes_only_that_tenant() {
    let head = WalHead::new();
    for tenant in ["tenant-a", "tenant-b"] {
        head.update(|store| {
            store.push_float(
                tenant,
                lbls(&[crate::test_support::TestLabelPair::new("__name__", "up")]),
                1_000,
                1.0,
            );
        });
    }

    head.delete_tenant("tenant-a");

    assert2::check!(
        head.series("tenant-a", &[], 0, 2_000)
            .await
            .unwrap()
            .is_empty()
    );
    assert2::check!(head.series("tenant-b", &[], 0, 2_000).await.unwrap().len() == 1);
}
}
