#![allow(dead_code)]

use std::sync::Arc;

use krabka_blockstore::{Labels, TenantId};
use krabka_metrics::{BucketSpan, NativeHistogram, ResetHint};

use crate::{
    EngineOpts, InMemoryMetricStore, InstantSample, PromqlEngine, QueryResult, SampleValue,
    conformance::testkit::metric_to_labels, error::Result,
};

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

mod empty_store;
mod eval_instant;
mod eval_instant_err;
mod eval_instant_nh;
mod instant_sample;
mod instant_sample_ext;
mod labels;
mod metric_store_with_empty_lookups;
mod nh;
mod query_result;
mod query_result_ext;
mod spans_and_counts;
mod store_with_classic_histogram;
mod store_with_labeled_series;
mod store_with_series;
mod store_with_series_multi;
mod tenant;
mod tenant_cardinality;
mod tenant_id;

pub(crate) use eval_instant::eval_instant;
pub(crate) use eval_instant_err::eval_instant_err;
pub(crate) use instant_sample_ext::InstantSampleExt;
pub(crate) use labels::labels;
pub(crate) use metric_store_with_empty_lookups::metric_store_with_empty_lookups;
pub(crate) use nh::{nh, two_sided_histogram};
pub(crate) use query_result_ext::QueryResultExt;
use spans_and_counts::spans_and_counts;
pub(crate) use spans_and_counts::{BucketCount, SpansAndCounts};
pub(crate) use store_with_series::{TestSample, store_with_series};
pub(crate) use store_with_series_multi::{TestSeriesValue, store_with_series_multi};
use tenant::TENANT;
pub(crate) use tenant_cardinality::{
    ExpectedCardinality, ExpectedLabelNameCount, ExpectedLabelValueCount, assert_tenant_cardinality,
};
pub(crate) use tenant_id::tenant_id;
