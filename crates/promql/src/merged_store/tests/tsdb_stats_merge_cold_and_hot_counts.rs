use super::*;

#[tokio::test]
pub(crate) async fn tsdb_stats_merge_cold_and_hot_counts() {
    let store = merged_store_with_samples(
        &[
            MergedFloatSample::builder()
                .metric("up")
                .job("api")
                .instance("")
                .cluster("")
                .timestamp_ms(10_000)
                .value(1.0)
                .build(),
            MergedFloatSample::builder()
                .metric("up")
                .job("api")
                .instance("")
                .cluster("")
                .timestamp_ms(20_000)
                .value(2.0)
                .build(),
        ],
        &[MergedFloatSample::builder()
            .metric("errors_total")
            .job("worker")
            .instance("")
            .cluster("")
            .timestamp_ms(30_000)
            .value(3.0)
            .build()],
    );
    let stats = store.tsdb_stats("tenant-a").await.unwrap();

    let stat = |name: &str, value: usize| NamedTsdbStat {
        name: name.to_string(),
        value,
    };
    assert2::assert!(
        stats
            == TsdbStats {
                head_stats: TsdbHeadStats {
                    num_series: 2,
                    num_samples: 3,
                    num_chunks: 2,
                    min_time: 10_000,
                    max_time: 30_000,
                },
                series_count_by_metric_name: vec![stat("errors_total", 1), stat("up", 1)],
                label_value_count_by_label_name: vec![stat("__name__", 2), stat("job", 2)],
                // Byte counts sum name.len() + value.len() per series:
                // "__name__"+"up" (10) + "__name__"+"errors_total" (20) = 30;
                // "job"+"api" (6) + "job"+"worker" (9) = 15.
                memory_in_bytes_by_label_name: vec![stat("__name__", 30), stat("job", 15)],
                series_count_by_label_value_pair: vec![
                    stat("__name__=errors_total", 1),
                    stat("__name__=up", 1),
                    stat("job=api", 1),
                    stat("job=worker", 1),
                ],
            }
    );
}
