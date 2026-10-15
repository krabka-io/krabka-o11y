use super::*;

mod cardinality_methods_merge_cold_and_hot_series {
use super::*;
use crate::test_support::{
    ExpectedCardinality, ExpectedLabelNameCount, ExpectedLabelValueCount, assert_tenant_cardinality,
};

#[tokio::test]
pub(crate) async fn cardinality_methods_merge_cold_and_hot_series() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    let api = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("instance", "a"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    let worker = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("instance", "b"),
        crate::test_support::TestLabelPair::new("job", "worker"),
    ]);
    cold.push_float("tenant-a", api.clone(), 10_000, 1.0);
    hot.push_float("tenant-a", worker.clone(), 20_000, 2.0);

    let store = MergedMetricStore::new(cold, hot);
    assert_tenant_cardinality(
        &store,
        ExpectedCardinality {
            active_series: vec![api, worker],
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
            ],
            label_value_counts: &[
                ExpectedLabelValueCount {
                    name: "__name__",
                    value: "up",
                    series_count: 2,
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
                    name: "job",
                    value: "api",
                    series_count: 1,
                },
                ExpectedLabelValueCount {
                    name: "job",
                    value: "worker",
                    series_count: 1,
                },
            ],
        },
    )
    .await;
}

}

mod created_timestamp_survives_cold_and_hot_merge {
use krabka_blockstore::BlockStore;
use krabka_metrics::{encode_float_samples, float_sample_schema};
use object_store::{ObjectStore, memory::InMemory};

use super::*;
use crate::block_store::MetricBlockStore;

#[tokio::test]
pub(crate) async fn created_timestamp_survives_cold_and_hot_merge() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut blocks = BlockStore::new(object_store, url::Url::parse("memory:///").unwrap());
    let series_labels = crate::test_support::labels::http_requests_total_api_labels();
    let fp = series_labels.fingerprint();
    let batch = encode_float_samples(&[krabka_metrics::FloatSampleRow {
        fingerprint: fp,
        timestamp_ms: 100_000,
        value: 5.0,
        start_timestamp_ms: Some(50_000),
    }])
    .unwrap();
    let block_meta = blocks
        .writer()
        .write_block(
            "tenant-a",
            "metrics/float/created.parquet",
            float_sample_schema(),
            &[batch],
        )
        .await
        .unwrap();
    blocks
        .index_mut()
        .add_series("tenant-a", fp, &series_labels.utf8_projection());
    blocks.index_mut().add_block(&block_meta);

    let mut hot = InMemoryMetricStore::new();
    hot.push_float_with_start_timestamp("tenant-a", series_labels, 200_000, 8.0, Some(50_000));
    let engine = PromqlEngine::new(
        Arc::new(MergedMetricStore::new(MetricBlockStore::new(blocks), hot)),
        EngineOpts::default(),
    );

    let result = engine
        .query_instant(
            &tenant_id("tenant-a"),
            "increase(http_requests_total[5m])",
            300_000,
        )
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 1);
    let SampleValue::Float(value) = &samples[0].value else {
        panic!("expected float");
    };
    assert2::assert!((*value - 40.0 / 3.0).abs() < 1e-9);
}

}

mod label_names_merges_cold_and_hot_series_metadata {
use super::*;

#[tokio::test]
pub(crate) async fn label_names_merges_cold_and_hot_series_metadata() {
    let store = merged_store_with_samples(
        &[MergedFloatSample::builder()
            .metric("up")
            .job("api")
            .instance("a")
            .cluster("")
            .timestamp_ms(10_000)
            .value(1.0)
            .build()],
        &[MergedFloatSample::builder()
            .metric("up")
            .job("api")
            .instance("")
            .cluster("prod")
            .timestamp_ms(20_000)
            .value(2.0)
            .build()],
    );
    let names = store.label_names("tenant-a", &[], 0, 30_000).await.unwrap();
    assert2::assert!(names == vec!["__name__", "cluster", "instance", "job"]);
}

}

mod label_values_merges_cold_and_hot_series_metadata {
use super::*;

#[tokio::test]
pub(crate) async fn label_values_merges_cold_and_hot_series_metadata() {
    let store = merged_store_with_samples(
        &[MergedFloatSample::builder()
            .metric("up")
            .job("api")
            .instance("")
            .cluster("")
            .timestamp_ms(10_000)
            .value(1.0)
            .build()],
        &[
            MergedFloatSample::builder()
                .metric("up")
                .job("worker")
                .instance("")
                .cluster("")
                .timestamp_ms(20_000)
                .value(2.0)
                .build(),
            MergedFloatSample::builder()
                .metric("up")
                .job("api")
                .instance("")
                .cluster("")
                .timestamp_ms(30_000)
                .value(3.0)
                .build(),
        ],
    );
    let values = store
        .label_values("tenant-a", "job", &[], 0, 30_000)
        .await
        .unwrap();
    assert2::assert!(values == vec!["api", "worker"]);
}

}

mod metadata_merges_cold_and_hot_records {
use super::*;

#[derive(Debug, PartialEq)]
struct MetadataFields<'a> {
    family_name: &'a str,
    metric_type: &'a str,
    help: &'a str,
    unit: &'a str,
}

#[tokio::test]
pub(crate) async fn metadata_merges_cold_and_hot_records() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    cold.push_metadata(
        "tenant-a",
        "requests_total",
        "counter",
        "requests",
        "requests",
    );
    cold.push_metadata("tenant-a", "up", "gauge", "availability", "");
    hot.push_metadata(
        "tenant-a",
        "requests_total",
        "counter",
        "requests",
        "requests",
    );
    hot.push_metadata(
        "tenant-a",
        "latency_seconds",
        "histogram",
        "latency",
        "seconds",
    );

    let store = MergedMetricStore::new(cold, hot);
    let metadata = store.metadata("tenant-a", None).await.unwrap().metadata;
    let fields = metadata
        .iter()
        .map(|record| MetadataFields {
            family_name: record.metric_family_name.as_str(),
            metric_type: record.metric_type.as_str(),
            help: record.help.as_str(),
            unit: record.unit.as_str(),
        })
        .collect::<Vec<_>>();

    assert2::assert!(
        fields
            == vec![
                MetadataFields {
                    family_name: "latency_seconds",
                    metric_type: "histogram",
                    help: "latency",
                    unit: "seconds",
                },
                MetadataFields {
                    family_name: "requests_total",
                    metric_type: "counter",
                    help: "requests",
                    unit: "requests",
                },
                MetadataFields {
                    family_name: "up",
                    metric_type: "gauge",
                    help: "availability",
                    unit: "",
                },
            ]
    );
}

}

mod tsdb_blocks_merges_cold_and_hot_blocks {
use super::*;

#[tokio::test]
pub(crate) async fn tsdb_blocks_merges_cold_and_hot_blocks() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    cold.push_tsdb_block("tenant-a", "cold-b", 30_000, 40_000, 3, 1);
    hot.push_tsdb_block("tenant-a", "hot-a", 10_000, 20_000, 2, 1);
    cold.push_tsdb_block("tenant-a", "cold-a", 10_000, 15_000, 1, 1);

    let store = MergedMetricStore::new(cold, hot);
    let blocks = store.tsdb_blocks("tenant-a").await.unwrap();
    let ids = blocks
        .iter()
        .map(|block| block.id.as_str())
        .collect::<Vec<_>>();

    assert2::assert!(ids == vec!["cold-a", "hot-a", "cold-b"]);
}

}

mod tsdb_stats_ignore_empty_side_min_time {
use super::*;

#[tokio::test]
pub(crate) async fn tsdb_stats_ignore_empty_side_min_time() {
    let mut hot_only = InMemoryMetricStore::new();
    hot_only.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "up"),
            crate::test_support::TestLabelPair::new("job", "api"),
        ]),
        40_000,
        1.0,
    );
    let store = MergedMetricStore::new(InMemoryMetricStore::new(), hot_only);
    let stats = store.tsdb_stats("tenant-a").await.unwrap();
    assert2::assert!(stats.head_stats.min_time == 40_000);

    let mut cold_only = InMemoryMetricStore::new();
    cold_only.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "up"),
            crate::test_support::TestLabelPair::new("job", "api"),
        ]),
        50_000,
        1.0,
    );
    let store = MergedMetricStore::new(cold_only, InMemoryMetricStore::new());
    let stats = store.tsdb_stats("tenant-a").await.unwrap();
    assert2::assert!(stats.head_stats.min_time == 50_000);
}

}

mod tsdb_stats_merge_cold_and_hot_counts {
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

}
