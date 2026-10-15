use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use krabka_blockstore::{Labels, MatchOp};
use krabka_metrics::{BucketSpan, NativeHistogram, ResetHint};
use krabka_query_frontend::QueryCache;

use super::*;
use crate::{
    AnnotatedQueryResult, Annotations, EngineOpts, PromqlEngine, QueryResult, RangeSeries,
    RangeSeriesSample, SampleValue,
    test_support::{labels, tenant_id},
};

mod cached_up_matrix;
mod concurrency_probe_executor;
mod frontend_requests;
mod manual_clock;
mod partial_recording_executor;
mod rank_recording_executor;
mod shard_calls;
mod unannotated;
mod unlabeled_matrix;
mod warning_executor;

mod execution_cases;
mod range_planning_cases;
mod range_merge_cases;
use cached_up_matrix::{FloatPoint, one_sample_matrix, up_api_matrix, up_range_query};
use concurrency_probe_executor::ConcurrencyProbeExecutor;
use frontend_requests::{split_up_request, two_shard_instant_request};
use manual_clock::ManualClock;
use native_histogram_with_positive_buckets::native_histogram_with_positive_buckets;
use partial_recording_executor::PartialRecordingExecutor;
use rank_recording_executor::RankRecordingExecutor;
use recording_executor::RecordingExecutor;
use shard_calls::{on_both_shards, shard_calls};
use unannotated::{unannotated, unlabeled_partial};
use unlabeled_matrix::UnlabeledMatrix;
use warning_executor::WarningExecutor;

fn range_series(labels: Labels, samples: Vec<RangeSeriesSample>) -> RangeSeries {
    RangeSeries {
        drop_name: false,
        start_timestamps_ms: std::collections::BTreeMap::new(),
        labels: labels.into(),
        samples,
    }
}

#[derive(bon::Builder)]
struct UpQueryAndResult {
    query: FrontendRangeQuery,
    result: AnnotatedQueryResult,
}

fn up_query_and_result() -> UpQueryAndResult {
    UpQueryAndResult::builder()
        .query(up_range_query(0, None))
        .result(one_sample_matrix(labels(&[
            crate::test_support::TestLabelPair::new("__name__", "up"),
        ])))
        .build()
}

async fn assert_ttl_hit_then_miss<
    C: QueryCache<AnnotatedQueryResult, Error = crate::PromqlError>,
>(
    cache: &C,
    clock: &ManualClock,
    query: &FrontendRangeQuery,
    result: &AnnotatedQueryResult,
    within_ttl_ms: i64,
    past_ttl_ms: i64,
) {
    let key = super::cache::range_cache_key("tenant-a", query);
    QueryCache::insert(cache, &key, result).await.unwrap();
    clock.advance(within_ttl_ms);
    assert2::assert!(QueryCache::get(cache, &key).await.unwrap() == Some(result.clone()));
    clock.advance(past_ttl_ms);
    assert2::assert!(QueryCache::get(cache, &key).await.unwrap() == None);
}

mod cache_semantics;

mod support;
pub(crate) use support::native_histogram_with_positive_buckets;
pub(crate) use support::range_query_plan_skips_unsupported_shards;
pub(crate) use support::recording_executor;
