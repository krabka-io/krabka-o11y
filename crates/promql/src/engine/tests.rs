use std::{collections::BTreeMap, sync::Arc};

use assert2::check;
use krabka_blockstore::Labels;
use krabka_metrics::{BucketSpan, NativeHistogram, ResetHint};
use krabka_units::prelude::*;

use super::{
    MAX_RESOLUTION_POINTS, MatrixSelectorAt, check_resolution_points, match_rate_range_call,
};
use crate::{
    AnnotatedQueryResult, EngineOpts, InMemoryMetricStore, MetricStore, PromqlEngine, PromqlError,
    QueryResult, SampleValue,
    test_support::{labels, tenant_id},
};

fn http_requests_counter_labels() -> Labels {
    labels(&[
        crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ])
}

fn push_float_counter_series(
    store: &mut InMemoryMetricStore,
    labels: Labels,
    factor: f64,
    last_step: i32,
) {
    for step in 0..=last_step {
        store.push_float(
            "t",
            labels.clone(),
            i64::from(step) * 60_000,
            f64::from(step) * factor,
        );
    }
}

struct QueryAt {
    query: &'static str,
    time_ms: i64,
}

#[derive(Clone, Copy, bon::Builder)]
struct FloatSampleInput {
    timestamp_ms: i64,
    value: f64,
}

struct LabeledFloatSeries {
    labels: Labels,
    samples: Vec<FloatSampleInput>,
}

trait FloatSampleSeed {
    fn push_into(self, store: &mut InMemoryMetricStore);
}

fn push_float_samples<S: FloatSampleSeed>(
    store: &mut InMemoryMetricStore,
    samples: impl IntoIterator<Item = S>,
) {
    for sample in samples {
        sample.push_into(store);
    }
}

fn push_float_series(
    store: &mut InMemoryMetricStore,
    series: impl IntoIterator<Item = LabeledFloatSeries>,
) {
    for series in series {
        for sample in series.samples {
            store.push_float(
                "t",
                series.labels.clone(),
                sample.timestamp_ms,
                sample.value,
            );
        }
    }
}

async fn query_range_matrix<S: MetricStore>(
    engine: &PromqlEngine<S>,
    query: &str,
    step_ms: i64,
) -> Vec<crate::RangeSeries> {
    let result = engine
        .query_range(
            &tenant_id("tenant-a"),
            query,
            60_000,
            180_000,
            millis(u32::try_from(step_ms).expect("step duration fits u32 milliseconds")),
        )
        .await
        .unwrap();
    let QueryResult::RangeMatrix(series) = result else {
        panic!("expected matrix");
    };
    series
}

async fn query_instant_vector<S: MetricStore>(
    engine: &PromqlEngine<S>,
    tenant: &str,
    query: &str,
    time_ms: i64,
) -> Vec<crate::InstantSample> {
    let result = engine
        .query_instant(&tenant_id(tenant), query, time_ms)
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    samples
}

async fn assert_call_planner_matches_interpreter<S: MetricStore>(
    engine: &PromqlEngine<S>,
    queries: &[QueryAt],
) {
    for query_at in queries {
        let parity = planned_and_interpreted(engine, query_at.query, query_at.time_ms).await;
        let via_operators = parity.via_operators;
        let via_interpreter = parity.via_interpreter;
        let via_operators = sort_instant_result(via_operators);
        let via_interpreter = sort_instant_result(via_interpreter);
        assert2::assert!(query_results_match(&via_interpreter, &via_operators));
    }
}

async fn assert_annotated_call_planner_matches_interpreter<S: MetricStore>(
    engine: &PromqlEngine<S>,
    query: &str,
    time_ms: i64,
) {
    let parity = annotated_planned_and_interpreted(engine, query, time_ms).await;
    let via_interpreter = fingerprint_sorted(parity.via_interpreter.result, query);
    let via_operators = fingerprint_sorted(parity.via_operators.result, query);
    assert2::assert!(instant_samples_match(&via_interpreter, &via_operators));
    assert2::assert!(parity.via_operators.annotations == parity.via_interpreter.annotations);
}

mod api_series_engines;
mod assert_single_float_sample;
mod assert_single_on_x_float_sample;
mod byte_strings_keep_identity_through_composed_queries;
mod case_values;
mod counter_warning_composed_paths_preserve_origin_and_position;
mod float_store;
mod instant_samples_match;
mod instant_vector;
mod lone_matrix_series;
mod memory_bytes_engine;
mod mixed_histogram_store;
mod native_histogram_store;
mod planned_and_interpreted;
mod sample_instances;
mod series_store;
mod target_info_engine;
mod zoned_queue_depth_engine;

mod binary_semantics;
mod counter_functions;
mod histogram_semantics;
mod info_function_cases;
mod instant_aggregation_cases;
mod instant_function_cases;
mod instant_selection;
mod planner_parity_core;
mod planner_parity_ranges;
mod range_semantics;
mod scalar_semantics;
mod subquery_grid;
mod vector_semantics;
use api_series_engines::{api_latency_engine, late_jump_engine, requests_counter_engine};
use assert_aggregate_nan_staleness::assert_aggregate_nan_staleness;
use assert_filled_many_side::assert_filled_many_side;
use assert_minmax_nan_ignoring::assert_minmax_nan_ignoring;
use assert_one_unnamed_float::{ExpectedLabel, assert_one_unnamed_float};
use assert_single_float_sample::{assert_single_float_sample, lone_unnamed_sample};
use assert_single_on_x_float_sample::{OnXHistogramStats, assert_on_x_histogram_stats};
use assert_sparse_aggregate_excludes_no_value::assert_sparse_aggregate_excludes_no_value;
use case_values::{
    CaseQuery, CaseValue, InstanceValueCase, QueryValueCase, assert_case_values,
    assert_signed_temperature_cases, push_cases,
};
use classic_bucket_store::classic_bucket_store;
use float_store::{FloatStore, default_engine, lookback_engine};
use instant_samples_match::{instant_samples_match, nan_equal_samples};
use instant_vector::{
    LoneValueQuery, assert_lone_unlabeled_value_at, assert_lone_value, assert_lone_value_at,
    instant_vector,
};
use lone_matrix_series::{ExpectedPoint, check_series_points, lone_matrix_series};
use memory_bytes_engine::{
    JobInstanceSample, has_job_memory_bytes_sample, job_memory_bytes_engine, memory_bytes_engine,
};
use mixed_histogram_store::{
    MixedMetricFloatSample, mixed_api_group_store, mixed_histogram_store, mixed_metric_store,
};
use native_histogram::native_histogram;

pub(crate) fn two_positive_bucket_histogram(count: f64, sum: f64) -> NativeHistogram {
    let mut histogram = native_histogram(count, sum);
    histogram.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 2,
    }];
    histogram.positive_counts = vec![1.0, 3.0];
    histogram
}

pub(crate) fn single_bucket_native_histogram(
    count: f64,
    sum: f64,
    zero_count: f64,
    bucket_count: f64,
) -> NativeHistogram {
    let mut histogram = native_histogram(count, sum);
    histogram.zero_count = zero_count;
    histogram.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 1,
    }];
    histogram.positive_counts = vec![bucket_count];
    histogram
}

pub(crate) fn push_named_job_instance_sample(
    store: &mut InMemoryMetricStore,
    metric: &str,
    job: &str,
    instance: &str,
    value: f64,
) {
    push_metric_sample(store, metric, Some(job), instance, value);
}

pub(crate) fn comparison_test_engine() -> PromqlEngine<InMemoryMetricStore> {
    FloatStore::default()
        .named_sample(
            "a",
            &[crate::test_support::TestLabelPair::new("x", "1")],
            10.0,
        )
        .engine()
}

pub(crate) fn push_metric_instance_sample(
    store: &mut InMemoryMetricStore,
    metric: &str,
    instance: &str,
    value: f64,
) {
    push_metric_sample(store, metric, None, instance, value);
}

fn push_metric_sample(
    store: &mut InMemoryMetricStore,
    metric: &str,
    job: Option<&str>,
    instance: &str,
    value: f64,
) {
    let mut pairs = vec![
        crate::test_support::TestLabelPair::new("__name__", metric),
        crate::test_support::TestLabelPair::new("instance", instance),
    ];
    if let Some(job) = job {
        pairs.push(crate::test_support::TestLabelPair::new("job", job));
    }
    store.push_float("tenant-a", labels(&pairs), 10_000, value);
}

pub(crate) fn push_metric_job_samples(
    store: &mut InMemoryMetricStore,
    samples: impl IntoIterator<Item = (&'static str, &'static str, f64)>,
) {
    for (metric, job, value) in samples {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", metric),
                crate::test_support::TestLabelPair::new("job", job),
            ]),
            10_000,
            value,
        );
    }
}

pub(crate) fn push_metadata_sample(store: &mut InMemoryMetricStore, instance: &str, value: f64) {
    store.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "requests_total"),
            crate::test_support::TestLabelPair::new("__type__", "counter"),
            crate::test_support::TestLabelPair::new("__unit__", "requests"),
            crate::test_support::TestLabelPair::new("instance", instance),
        ]),
        10_000,
        value,
    );
}

use native_histogram_store::{
    InstanceHistograms, assert_histogram_series_reduction, instance_histogram_store,
    native_histogram_store, signed_bucket_histogram_engine,
    store_with_request_duration_histogram_at, two_bucket_histogram_store,
};
use planned_and_interpreted::{
    SortedParity, annotated_planned_and_interpreted, fingerprint_sorted, planned_and_interpreted,
    sorted_planned_and_interpreted,
};
use query_results_match::query_results_match;
use queue_depth_engine::queue_depth_engine;
use range_matrices_match::range_matrices_match;
use range_query_routes_through_planner::range_query_routes_through_planner;
use region_info_engine::{InstanceRequests, region_info_engine};
#[cfg(feature = "experimental-functions")]
use sample_instances::{sample_instances, selected_memory_instances};
use series_store::{SeriesFixture, queue_depth_store, up_series_store};

fn three_point_gauge_store(metric: &str) -> InMemoryMetricStore {
    SeriesFixture::new(labels(&[
        crate::test_support::TestLabelPair::new("__name__", metric),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]))
    .at(0_i64, 1.0)
    .at(60_000, 3.0)
    .at(120_000, 5.0)
    .store()
}

fn sample_with_label<'a>(
    samples: &'a [crate::InstantSample],
    name: &str,
    value: &str,
) -> &'a crate::InstantSample {
    samples
        .iter()
        .find(|sample| sample.labels.get(name) == Some(value))
        .expect("the sample has the requested label")
}
use set_op_store::set_op_store;
use short_lookback_up_engine::short_lookback_up_engine;
use sort_instant_result::sort_instant_result;
use target_info_engine::{info_metrics_engine, target_info_engine};
use zoned_queue_depth_engine::{
    ZonedQueueDepth, standard_zoned_queue_depth_series, zoned_queue_depth_engine,
};

struct TopKProjection<'a> {
    metric_name: Option<&'a str>,
    instance: Option<&'a str>,
    value: f64,
}

fn topk_projection(samples: &[crate::InstantSample]) -> Vec<TopKProjection<'_>> {
    let mut projection = samples
        .iter()
        .map(|sample| TopKProjection {
            metric_name: sample.labels.get("__name__"),
            instance: sample.labels.get("instance"),
            value: float_value(&sample.value),
        })
        .collect::<Vec<_>>();
    projection.sort_by_key(|sample| sample.instance);
    projection
}

async fn assert_label_transform<S: MetricStore>(
    engine: &PromqlEngine<S>,
    query: &str,
    result_label: &str,
    expected_result: &str,
    preserved_label: &str,
    expected_preserved: &str,
) {
    let samples = query_instant_vector(&engine, "tenant-a", query, 10_000).await;
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].labels.get(result_label) == Some(expected_result));
    assert2::assert!(samples[0].labels.get(preserved_label) == Some(expected_preserved));
    assert2::assert!(approx_eq(float_value(&samples[0].value), 1.0));
}

fn approx_eq(left: f64, right: f64) -> bool {
    if !left.is_finite() || !right.is_finite() {
        return left == right;
    }
    // Relative tolerance: an absolute `f64::EPSILON` bound is too tight for
    // magnitudes above ~1 — a Kahan/Welford-compensated fold (matching
    // Prometheus) rounds in the last ULP, e.g. a population variance of 4.0
    // lands at 3.999999999999_9996. Scale the bound by operand magnitude.
    let scale = left.abs().max(right.abs()).max(1.0);
    (left - right).abs() <= f64::EPSILON * 4.0 * scale
}

fn float_value(value: &SampleValue) -> f64 {
    match value {
        SampleValue::Float(value) => *value,
        SampleValue::Histogram(_) => panic!("expected float sample"),
    }
}

fn stale_nan() -> f64 {
    f64::from_bits(0x7ff0_0000_0000_0002)
}

mod histogram_edge_cases;

mod planner_edge_cases;

mod aggregation_edge_cases;

mod range_aggregation_edges;

mod support;
pub(crate) use support::{
    assert_aggregate_nan_staleness, assert_filled_many_side, assert_minmax_nan_ignoring,
    assert_one_unnamed_float, assert_sparse_aggregate_excludes_no_value, classic_bucket_store,
    instant_experimental_functions_require_feature, native_histogram, query_results_match,
    queue_depth_engine, range_matrices_match, range_query_routes_through_planner,
    region_info_engine, set_op_store, short_lookback_up_engine, sort_instant_result,
};
