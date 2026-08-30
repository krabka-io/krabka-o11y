use super::*;

#[tokio::test]
pub(crate) async fn store_cardinality_and_tsdb_stats_include_float_and_hist_series() {
    let (store, _) = store_with_float_and_hist_series();

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
