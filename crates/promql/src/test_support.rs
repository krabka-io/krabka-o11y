#![allow(dead_code)]

use std::sync::Arc;

use krabka_blockstore::{Labels, TenantId};
use krabka_metrics::{BucketSpan, NativeHistogram, ResetHint};

use crate::{
    EngineOpts, InMemoryMetricStore, InstantSample, PromqlEngine, QueryResult, SampleValue,
    conformance::testkit::metric_to_labels, error::Result,
};

pub(crate) fn native_histogram_fixture(
    schema: i8,
    is_float: bool,
    reset_hint: ResetHint,
    custom_values: Option<Vec<f64>>,
    start_timestamp_ms: Option<i64>,
) -> NativeHistogram {
    NativeHistogram {
        schema,
        is_float,
        reset_hint,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count: 0.0,
        sum: 0.0,
        positive_spans: Vec::new(),
        positive_counts: Vec::new(),
        negative_spans: Vec::new(),
        negative_counts: Vec::new(),
        custom_values,
        start_timestamp_ms,
    }
}

#[cfg(test)]
pub(crate) mod http {
    use axum::{body::Body, http::Request};

    pub(crate) fn empty_tenant_request(uri: &str, tenant: &str) -> Request<Body> {
        Request::builder()
            .uri(uri)
            .header("X-Scope-OrgID", tenant)
            .body(Body::empty())
            .unwrap()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn store_with_series_round_trips_through_eval_instant() {
        let store = store_with_series(
            "up",
            &[TestSample::builder().timestamp_ms(0).value(1.0).build()],
        );
        let result = eval_instant(&store, "up", 0).await;

        assert2::assert!((result.single().value_f64() - 1.0).abs() < f64::EPSILON);
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct TestLabelPair<'a> {
    pub(crate) name: &'a str,
    pub(crate) value: &'a str,
}

#[cfg(test)]
impl<'a> TestLabelPair<'a> {
    pub(crate) const fn new(name: &'a str, value: &'a str) -> Self {
        Self { name, value }
    }
}
pub(crate) mod labels;
mod metric_store_with_empty_lookups;
mod nh;
mod query_result;
mod spans_and_counts;
mod store_with_series;
mod store_with_series_multi;
mod tenant_cardinality;
pub(crate) use labels::{labels, up_instance_labels};
pub(crate) use metric_store_with_empty_lookups::metric_store_with_empty_lookups;
pub(crate) use nh::{nh, two_sided_histogram};
use spans_and_counts::spans_and_counts;
pub(crate) use spans_and_counts::{BucketCount, SpansAndCounts};
pub(crate) use store_with_series::{TestSample, store_with_series, up_api_store};
pub(crate) use store_with_series_multi::{TestSeriesValue, store_with_series_multi};
pub(crate) use tenant_cardinality::{
    ExpectedCardinality, ExpectedLabelNameCount, ExpectedLabelValueCount, assert_tenant_cardinality,
};

pub(crate) const TENANT: &str = "test";

pub(crate) fn empty_store() -> InMemoryMetricStore {
    InMemoryMetricStore::new()
}

pub(crate) async fn eval_instant(
    store: &InMemoryMetricStore,
    query: &str,
    ts_ms: i64,
) -> QueryResult {
    eval_instant_err(store, query, ts_ms).await.unwrap()
}

pub(crate) async fn eval_instant_err(
    store: &InMemoryMetricStore,
    query: &str,
    ts_ms: i64,
) -> Result<QueryResult> {
    let engine = PromqlEngine::new(Arc::new(store.clone()), EngineOpts::default());
    engine.query_instant(&tenant_id(TENANT), query, ts_ms).await
}

pub(crate) fn eval_instant_nh(name: &str, histogram: &NativeHistogram) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    let mut labels = Labels::new();
    labels.insert("__name__", name);
    store.push_histogram(TENANT, labels, 0, histogram.clone());
    store
}

impl InstantSampleExt for InstantSample {
    fn value_f64(&self) -> f64 {
        match &self.value {
            SampleValue::Float(value) => *value,
            SampleValue::Histogram(_) => panic!("expected float sample"),
        }
    }
}

pub(crate) trait InstantSampleExt {
    fn value_f64(&self) -> f64;
}

pub(crate) trait QueryResultExt {
    fn single(&self) -> &InstantSample;
    fn as_scalar(&self) -> f64;
    fn values_f64(&self) -> Vec<f64>;
    fn is_empty(&self) -> bool;
    fn iter(&self) -> std::slice::Iter<'_, InstantSample>;
    fn len(&self) -> usize;
}

/// Builds a tenant id for a test from a name the test knows is valid.
pub(crate) fn tenant_id(name: &str) -> TenantId {
    TenantId::new(name).expect("a test tenant name is a valid tenant id")
}

pub(crate) fn store_with_classic_histogram() -> InMemoryMetricStore {
    store_with_series_multi(&[
        TestSeriesValue::builder()
            .selector("http_request_duration_seconds_bucket{le=\"0.1\"}")
            .value(0.0)
            .build(),
        TestSeriesValue::builder()
            .selector("http_request_duration_seconds_bucket{le=\"0.2\"}")
            .value(1.0)
            .build(),
        TestSeriesValue::builder()
            .selector("http_request_duration_seconds_bucket{le=\"0.4\"}")
            .value(3.0)
            .build(),
        TestSeriesValue::builder()
            .selector("http_request_duration_seconds_bucket{le=\"+Inf\"}")
            .value(3.0)
            .build(),
    ])
}

pub(crate) fn store_with_labeled_series(
    name: &str,
    labels: &[TestLabelPair<'_>],
    value: f64,
) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    let mut label_set = Labels::new();
    label_set.insert("__name__", name);
    for pair in labels {
        label_set.insert(pair.name, pair.value);
    }
    store.push_float(TENANT, label_set, 0, value);
    store
}
