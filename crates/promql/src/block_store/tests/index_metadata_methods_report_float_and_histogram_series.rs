use super::*;

#[tokio::test]
pub(crate) async fn index_metadata_methods_report_float_and_histogram_series() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let mut floats = BlockStore::new(object_store.clone(), base.clone());
    let mut histograms = BlockStore::new(object_store, base);

    let up = labels(&[("__name__", "up"), ("instance", "a"), ("job", "api")]);
    let latency = labels(&[
        ("__name__", "http_request_duration_seconds"),
        ("instance", "b"),
        ("job", "api"),
        ("le", "0.5"),
    ]);
    floats
        .index_mut()
        .add_series("tenant-a", up.fingerprint(), &up);
    histograms
        .index_mut()
        .add_series("tenant-a", latency.fingerprint(), &latency);
    let store = MetricBlockStore::with_histograms(floats, histograms);

    let names = store.label_names("tenant-a", &[], 0, 10_000).await.unwrap();
    assert2::assert!(names == vec!["__name__", "instance", "job", "le"]);

    let job_values = store
        .label_values("tenant-a", "job", &[], 0, 10_000)
        .await
        .unwrap();
    assert2::assert!(job_values == vec!["api"]);
    let instance_values = store
        .label_values("tenant-a", "instance", &[], 0, 10_000)
        .await
        .unwrap();
    assert2::assert!(instance_values == vec!["a", "b"]);

    let mut active_series = store.cardinality_active_series("tenant-a").await.unwrap();
    active_series.sort_by_key(|labels| labels.get("instance").unwrap_or("").to_string());
    assert2::assert!(active_series == vec![up, latency]);

    let label_names = store.cardinality_label_names("tenant-a").await.unwrap();
    let name_counts = label_names
        .iter()
        .map(|stat| (stat.name.as_str(), stat.series_count))
        .collect::<Vec<_>>();
    assert2::assert!(name_counts == vec![("__name__", 2), ("instance", 2), ("job", 2), ("le", 1)]);

    let label_values = store.cardinality_label_values("tenant-a").await.unwrap();
    let value_counts = label_values
        .iter()
        .map(|stat| {
            (
                stat.label_name.as_str(),
                stat.label_value.as_str(),
                stat.series_count,
            )
        })
        .collect::<Vec<_>>();
    assert2::assert!(
        value_counts
            == vec![
                ("job", "api", 2),
                ("__name__", "http_request_duration_seconds", 1),
                ("__name__", "up", 1),
                ("instance", "a", 1),
                ("instance", "b", 1),
                ("le", "0.5", 1),
            ]
    );

    let stats = store.tsdb_stats("tenant-a").await.unwrap();
    assert2::assert!(
        stats
            == TsdbStats {
                head_stats: TsdbHeadStats {
                    num_series: 2,
                    num_samples: 0,
                    num_chunks: 2,
                    min_time: 0,
                    max_time: 0,
                },
                series_count_by_metric_name: expected_stats(&[
                    ("http_request_duration_seconds", 1),
                    ("up", 1),
                ]),
                label_value_count_by_label_name: expected_stats(&[
                    ("__name__", 2),
                    ("instance", 2),
                    ("job", 1),
                    ("le", 1),
                ]),
                memory_in_bytes_by_label_name: expected_stats(&[
                    ("__name__", 47),
                    ("instance", 18),
                    ("job", 12),
                    ("le", 5),
                ]),
                series_count_by_label_value_pair: expected_stats(&[
                    ("job=api", 2),
                    ("__name__=http_request_duration_seconds", 1),
                    ("__name__=up", 1),
                    ("instance=a", 1),
                    ("instance=b", 1),
                    ("le=0.5", 1),
                ]),
            }
    );
}
