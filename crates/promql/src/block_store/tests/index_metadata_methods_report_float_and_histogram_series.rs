use super::*;
use crate::test_support::{
    ExpectedCardinality, ExpectedLabelNameCount, ExpectedLabelValueCount,
    assert_tenant_cardinality, up_instance_labels,
};

#[tokio::test]
pub(crate) async fn index_metadata_methods_report_float_and_histogram_series() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let mut floats = BlockStore::new(object_store.clone(), base.clone());
    let mut histograms = BlockStore::new(object_store, base);

    let up = up_instance_labels("a");
    let latency = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "http_request_duration_seconds"),
        crate::test_support::TestLabelPair::new("instance", "b"),
        crate::test_support::TestLabelPair::new("job", "api"),
        crate::test_support::TestLabelPair::new("le", "0.5"),
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

    assert_tenant_cardinality(
        &store,
        ExpectedCardinality {
            active_series: vec![up.into(), latency.into()],
            label_name_counts: &[
                ExpectedLabelNameCount {
                    name: "__name__",
                    series_count: 2,
                },
                ExpectedLabelNameCount {
                    name: "instance",
                    series_count: 2,
                },
                ExpectedLabelNameCount {
                    name: "job",
                    series_count: 2,
                },
                ExpectedLabelNameCount {
                    name: "le",
                    series_count: 1,
                },
            ],
            label_value_counts: &[
                ExpectedLabelValueCount {
                    name: "job",
                    value: "api",
                    series_count: 2,
                },
                ExpectedLabelValueCount {
                    name: "__name__",
                    value: "http_request_duration_seconds",
                    series_count: 1,
                },
                ExpectedLabelValueCount {
                    name: "__name__",
                    value: "up",
                    series_count: 1,
                },
                ExpectedLabelValueCount {
                    name: "instance",
                    value: "a",
                    series_count: 1,
                },
                ExpectedLabelValueCount {
                    name: "instance",
                    value: "b",
                    series_count: 1,
                },
                ExpectedLabelValueCount {
                    name: "le",
                    value: "0.5",
                    series_count: 1,
                },
            ],
        },
    )
    .await;

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
                    ExpectedStat {
                        name: "http_request_duration_seconds",
                        value: 1
                    },
                    ExpectedStat {
                        name: "up",
                        value: 1
                    },
                ]),
                label_value_count_by_label_name: expected_stats(&[
                    ExpectedStat {
                        name: "__name__",
                        value: 2
                    },
                    ExpectedStat {
                        name: "instance",
                        value: 2
                    },
                    ExpectedStat {
                        name: "job",
                        value: 1
                    },
                    ExpectedStat {
                        name: "le",
                        value: 1
                    },
                ]),
                memory_in_bytes_by_label_name: expected_stats(&[
                    ExpectedStat {
                        name: "__name__",
                        value: 47
                    },
                    ExpectedStat {
                        name: "instance",
                        value: 18
                    },
                    ExpectedStat {
                        name: "job",
                        value: 12
                    },
                    ExpectedStat {
                        name: "le",
                        value: 5
                    },
                ]),
                series_count_by_label_value_pair: expected_stats(&[
                    ExpectedStat {
                        name: "job=api",
                        value: 2
                    },
                    ExpectedStat {
                        name: "__name__=http_request_duration_seconds",
                        value: 1
                    },
                    ExpectedStat {
                        name: "__name__=up",
                        value: 1
                    },
                    ExpectedStat {
                        name: "instance=a",
                        value: 1
                    },
                    ExpectedStat {
                        name: "instance=b",
                        value: 1
                    },
                    ExpectedStat {
                        name: "le=0.5",
                        value: 1
                    },
                ]),
            }
    );
}
