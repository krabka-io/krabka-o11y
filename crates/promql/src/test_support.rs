#![allow(dead_code)]

use std::sync::Arc;

use krabka_blockstore::Labels;
use krabka_metrics::{BucketSpan, NativeHistogram, ResetHint};

use crate::{
    EngineOpts, InMemoryMetricStore, InstantSample, PromqlEngine, QueryResult, SampleValue,
    conformance::testkit::metric_to_labels, error::Result,
};

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn store_with_series_round_trips_through_eval_instant() {
        let store = store_with_series("up", &[(0, 1.0)]);
        let result = eval_instant(&store, "up", 0).await;

        assert2::assert!((result.single().value_f64() - 1.0).abs() < f64::EPSILON);
    }
}

// === split-modules: generated submodules ===
mod empty_store;
mod eval_instant;
mod eval_instant_err;
mod eval_instant_nh;
mod instant_sample;
mod instant_sample_ext;
mod nh;
mod query_result;
mod query_result_ext;
mod spans_and_counts;
mod store_with_classic_histogram;
mod store_with_labeled_series;
mod store_with_series;
mod store_with_series_multi;
mod tenant;

pub(crate) use eval_instant::eval_instant;
pub(crate) use eval_instant_err::eval_instant_err;
pub(crate) use instant_sample_ext::InstantSampleExt;
pub(crate) use query_result_ext::QueryResultExt;
use spans_and_counts::spans_and_counts;
pub(crate) use store_with_series::store_with_series;
pub(crate) use store_with_series_multi::store_with_series_multi;
use tenant::TENANT;
