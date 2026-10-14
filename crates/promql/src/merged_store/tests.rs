use std::sync::Arc;

use crate::{
    EngineOpts, ExemplarRecord, InMemoryMetricStore, InstantSample, MergedMetricStore, MetricStore,
    NamedTsdbStat, PromqlEngine, PromqlLabels as Labels, QueryResult, SampleValue, TsdbHeadStats,
    TsdbStats, test_support::tenant_id,
};

mod cardinality_methods_merge_cold_and_hot_series;
mod created_timestamp_survives_cold_and_hot_merge;
mod duplicate_samples_keep_hot_values;
mod exemplars_merges_cold_and_hot_records;
mod histograms_in_either_store_are_found;
mod instant_query_uses_hot_sample_newer_than_compacted_sample;
mod label_names_merges_cold_and_hot_series_metadata;
mod label_values_merges_cold_and_hot_series_metadata;
mod labels;
mod metadata_merges_cold_and_hot_records;
mod min_present_time_preserves_legitimate_zero_min_time;
mod range_query_counts_sample_present_in_both_stores_once;
mod tsdb_blocks_merges_cold_and_hot_blocks;
mod tsdb_stats_ignore_empty_side_min_time;
mod tsdb_stats_merge_cold_and_hot_counts;

use labels::labels;

#[derive(bon::Builder)]
pub(crate) struct MergedFloatSample {
    pub(crate) metric: &'static str,
    pub(crate) job: &'static str,
    pub(crate) instance: &'static str,
    pub(crate) cluster: &'static str,
    pub(crate) timestamp_ms: i64,
    pub(crate) value: f64,
}

fn store_with_samples(samples: &[MergedFloatSample]) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    for sample in samples {
        let mut label_pairs = vec![
            crate::test_support::TestLabelPair::new("__name__", sample.metric),
            crate::test_support::TestLabelPair::new("job", sample.job),
        ];
        if !sample.instance.is_empty() {
            label_pairs.push(crate::test_support::TestLabelPair::new(
                "instance",
                sample.instance,
            ));
        }
        if !sample.cluster.is_empty() {
            label_pairs.push(crate::test_support::TestLabelPair::new(
                "cluster",
                sample.cluster,
            ));
        }
        store.push_float(
            "tenant-a",
            labels(&label_pairs),
            sample.timestamp_ms,
            sample.value,
        );
    }
    store
}

fn merged_store_with_samples(
    cold_samples: &[MergedFloatSample],
    hot_samples: &[MergedFloatSample],
) -> MergedMetricStore<InMemoryMetricStore, InMemoryMetricStore> {
    MergedMetricStore::new(
        store_with_samples(cold_samples),
        store_with_samples(hot_samples),
    )
}

fn up_sample(timestamp_ms: i64, value: f64) -> MergedFloatSample {
    MergedFloatSample::builder()
        .metric("up")
        .job("api")
        .instance("")
        .cluster("")
        .timestamp_ms(timestamp_ms)
        .value(value)
        .build()
}
