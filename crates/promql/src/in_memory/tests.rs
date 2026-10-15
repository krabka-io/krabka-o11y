use arrow::{array::AsArray, datatypes::Int64Type};
use assert2::check;
use krabka_blockstore::MatchOp;
use krabka_metrics::{SamplePayload, WalExemplar, WalRecord};

use super::*;
use crate::{
    EngineOpts, PromqlEngine, PromqlError, PromqlLabels as Labels, PromqlMatcher as LabelMatcher,
    QueryResult, SampleValue, WalHead,
    store::{
        LabelNameCardinality, LabelValueCardinality, MetricStore, NamedTsdbStat, ScanResult,
        TsdbHeadStats,
    },
    test_support::tenant_id,
};

mod count_rows;
mod float_record;
mod interned_series_labels;
mod retention;
mod wal_batch_ledger;

use assert_query_shard_selects::assert_query_shard_selects;
use count_rows::{count_rows, count_up_float_rows};
use float_record::{float_record, float_sample_row};
use interned_series_labels::{SummaryLedger, SummaryLedgerEntry};
use lbls::lbls;
mod query_sharding;
mod row_windows;
mod scan_queries;
use store_with_float_and_hist_series::store_with_float_and_hist_series;

use crate::testkit::count_two_sum_three_histogram;

fn expected_label_memory_stats() -> Vec<NamedTsdbStat> {
    vec![
        NamedTsdbStat {
            name: "__name__".to_string(),
            value: 43,
        },
        NamedTsdbStat {
            name: "job".to_string(),
            value: 21,
        },
    ]
}

fn expected_label_name_cardinality() -> Vec<LabelNameCardinality> {
    vec![
        LabelNameCardinality {
            name: "__name__".to_string(),
            series_count: 3,
        },
        LabelNameCardinality {
            name: "job".to_string(),
            series_count: 3,
        },
    ]
}

fn expected_label_pair_stats() -> Vec<NamedTsdbStat> {
    vec![
        NamedTsdbStat {
            name: "__name__=up".to_string(),
            value: 2,
        },
        NamedTsdbStat {
            name: "job=api".to_string(),
            value: 2,
        },
        NamedTsdbStat {
            name: "__name__=latency_seconds".to_string(),
            value: 1,
        },
        NamedTsdbStat {
            name: "job=worker".to_string(),
            value: 1,
        },
    ]
}

fn expected_label_value_cardinality() -> Vec<LabelValueCardinality> {
    vec![
        LabelValueCardinality {
            label_name: "__name__".to_string(),
            label_value: "up".to_string(),
            series_count: 2,
        },
        LabelValueCardinality {
            label_name: "job".to_string(),
            label_value: "api".to_string(),
            series_count: 2,
        },
        LabelValueCardinality {
            label_name: "__name__".to_string(),
            label_value: "latency_seconds".to_string(),
            series_count: 1,
        },
        LabelValueCardinality {
            label_name: "job".to_string(),
            label_value: "worker".to_string(),
            series_count: 1,
        },
    ]
}

fn expected_label_value_count_stats() -> Vec<NamedTsdbStat> {
    vec![
        NamedTsdbStat {
            name: "__name__".to_string(),
            value: 2,
        },
        NamedTsdbStat {
            name: "job".to_string(),
            value: 2,
        },
    ]
}

fn expected_metric_name_stats() -> Vec<NamedTsdbStat> {
    vec![
        NamedTsdbStat {
            name: "up".to_string(),
            value: 2,
        },
        NamedTsdbStat {
            name: "latency_seconds".to_string(),
            value: 1,
        },
    ]
}

pub(crate) struct HeadWithUpSeries {
    pub(crate) head: WalHead,
    pub(crate) series: Labels,
}

pub(crate) fn up_series() -> Labels {
    lbls(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ])
}

pub(crate) async fn assert_up_query_value<S: MetricStore + 'static>(store: S) {
    let engine = PromqlEngine::new(std::sync::Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(&tenant_id("tenant-a"), "up", 10_000)
        .await
        .expect("query");
    let QueryResult::InstantVector(vector) = result else {
        panic!("expected vector");
    };
    assert2::assert!(vector[0].value == SampleValue::Float(1.0));
}

pub(crate) fn head_with_initial_up_sample() -> HeadWithUpSeries {
    let head = WalHead::new();
    let series = lbls(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    head.apply_wal_record(&float_record("t", &series, 1_000, 1.0));
    HeadWithUpSeries { head, series }
}

pub(crate) fn push_up_sample(
    store: &mut InMemoryMetricStore,
    job: &str,
    timestamp_ms: i64,
    value: f64,
) {
    store.push_float(
        "t",
        lbls(&[
            crate::test_support::TestLabelPair::new("__name__", "up"),
            crate::test_support::TestLabelPair::new("job", job),
        ]),
        timestamp_ms,
        value,
    );
}

mod wal_batch_cases;

mod support;
pub(crate) use support::{assert_query_shard_selects, lbls, store_with_float_and_hist_series};
