use super::*;

#[tokio::test]
pub(crate) async fn wal_head_delegates_metadata_cardinality_stats_and_blocks() {
    let (mut store, up_api) = store_with_float_and_hist_series();
    store.set_retention(millis(12_345));
    store.push_exemplar(
        "tenant-a",
        up_api.clone(),
        lbls(&[("trace_id", "abc")]),
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
            .unwrap()[0]
            .labels
            == lbls(&[("trace_id", "abc")])
    );
    check!(head.metadata("tenant-a", Some("up")).await.unwrap()[0].help == "Target health.");
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
