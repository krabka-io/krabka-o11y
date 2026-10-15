use std::sync::Arc;

use crate::{
    EngineOpts, ExemplarRecord, InMemoryMetricStore, InstantSample, MergedMetricStore, MetricStore,
    NamedTsdbStat, PromqlEngine, PromqlLabels as Labels, QueryResult, SampleValue, TsdbHeadStats,
    TsdbStats, test_support::tenant_id,
};


mod data_merge_cases;
mod metadata_merge_cases;
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

mod support;
pub(crate) use support::labels;
pub(crate) use support::min_present_time_preserves_legitimate_zero_min_time;
