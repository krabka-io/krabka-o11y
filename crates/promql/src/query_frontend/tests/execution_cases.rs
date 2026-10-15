use super::*;

mod frontend_range_execution_dispatches_subqueries_concurrently {
use super::{cached_up_matrix::up_api_series, *};

#[tokio::test]
pub(crate) async fn frontend_range_execution_dispatches_subqueries_concurrently() {
    // 4 splits over [0, 720_000] with a 180_000 split interval and 60_000
    // step, times 1 shard => 4 independent sub-queries.
    let planned = plan_range_query(
        "up",
        0,
        720_000,
        millis(60_000),
        QueryFrontendOptions {
            split_interval: millis(180_000),
            shard_count: 1,
        },
    )
    .unwrap();
    let width = planned.len();
    assert2::assert!(width >= 2);

    let executor = ConcurrencyProbeExecutor::new(width);
    let cache = QueryFrontendCache::default();

    let execution = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        execute_planned_range_queries(
            &executor,
            &cache,
            &tenant_id("tenant-a"),
            planned.clone(),
            krabka_query_frontend::AdmissionLimits::default(),
        ),
    )
    .await
    .expect("parallel fan-out must not block on the per-subquery barrier")
    .unwrap();
    let results = execution.results;
    let annotations = execution.annotations;

    // Every planned sub-query was dispatched exactly once.
    let mut dispatched = executor
        .calls
        .lock()
        .expect("probe executor calls poisoned")
        .clone();
    dispatched.sort_by_key(|query| query.start_ms);
    let mut expected = planned.clone();
    expected.sort_by_key(|query| query.start_ms);
    assert2::assert!(dispatched == expected);
    assert2::assert!(annotations == Annotations::new());

    // Stitched result is identical to a deterministic sequential merge,
    // independent of completion order.
    let stitched =
        merge_range_query_results_with_reducer(results.clone(), QueryShardReducer::First).unwrap();
    let mut sequential = Vec::new();
    for subquery in &planned {
        let point = FloatPoint::builder()
            .ts_ms(subquery.start_ms)
            .value(1.0)
            .build();
        sequential.push(QueryResult::RangeMatrix(vec![up_api_series(&[point])]));
    }
    let sequential_merge =
        merge_range_query_results_with_reducer(sequential, QueryShardReducer::First).unwrap();
    assert2::assert!(stitched == sequential_merge);
}

}

mod frontend_range_execution_reduces_sharded_avg_from_sum_and_count_partials {
use super::*;

#[tokio::test]
pub(crate) async fn frontend_range_execution_reduces_sharded_avg_from_sum_and_count_partials() {
    let cache = QueryFrontendCache::default();
    let executor = PartialRecordingExecutor::average();

    let result =
        execute_range_query_frontend(&executor, &cache, &two_shard_instant_request("avg(up)"))
            .await
            .unwrap();

    let calls = executor
        .calls
        .lock()
        .expect("avg partial executor calls poisoned")
        .clone();
    assert2::assert!(shard_calls(&calls) == on_both_shards(&["sum(up)", "count(up)"]));
    assert2::assert!(
        result
            == unannotated(QueryResult::RangeMatrix(vec![RangeSeries {
                drop_name: false,
                start_timestamps_ms: std::collections::BTreeMap::new(),
                labels: labels(&[]).into(),
                samples: vec![RangeSeriesSample::new(0, SampleValue::Float(4.0))],
            }]))
    );
}

}

mod frontend_range_execution_reduces_sharded_stddev_from_moment_partials {
use super::*;

#[tokio::test]
pub(crate) async fn frontend_range_execution_reduces_sharded_stddev_from_moment_partials() {
    let cache = QueryFrontendCache::default();
    let executor = PartialRecordingExecutor::moment();

    let result =
        execute_range_query_frontend(&executor, &cache, &two_shard_instant_request("stddev(up)"))
            .await
            .unwrap();

    let QueryResult::RangeMatrix(series) = result.result else {
        panic!("stddev range matrix");
    };
    let SampleValue::Float(value) = &series[0].samples[0].value else {
        panic!("stddev float sample");
    };
    assert2::assert!((*value - (38.0_f64 / 3.0).sqrt()).abs() < 1e-9);
}

}

mod frontend_range_execution_reduces_sharded_stdvar_from_moment_partials {
use super::*;

#[tokio::test]
pub(crate) async fn frontend_range_execution_reduces_sharded_stdvar_from_moment_partials() {
    let cache = QueryFrontendCache::default();
    let executor = PartialRecordingExecutor::moment();

    let result =
        execute_range_query_frontend(&executor, &cache, &two_shard_instant_request("stdvar(up)"))
            .await
            .unwrap();

    let calls = executor
        .calls
        .lock()
        .expect("moment partial executor calls poisoned")
        .clone();
    assert2::assert!(
        shard_calls(&calls) == on_both_shards(&["sum(up)", "count(up)", "sum((up) * (up))"])
    );
    let QueryResult::RangeMatrix(series) = result.result else {
        panic!("stdvar range matrix");
    };
    let SampleValue::Float(value) = &series[0].samples[0].value else {
        panic!("stdvar float sample");
    };
    assert2::assert!((*value - (38.0 / 3.0)).abs() < 1e-9);
}

}

mod frontend_range_execution_reduces_sharded_topk_from_rank_candidates {
use super::*;

#[derive(Debug, PartialEq)]
struct SelectedTopkValue {
    series: String,
    value: f64,
}

#[tokio::test]
pub(crate) async fn frontend_range_execution_reduces_sharded_topk_from_rank_candidates() {
    let cache = QueryFrontendCache::default();
    let executor = RankRecordingExecutor::default();

    let result =
        execute_range_query_frontend(&executor, &cache, &two_shard_instant_request("topk(2, up)"))
            .await
            .unwrap();

    let calls = executor
        .calls
        .lock()
        .expect("rank executor calls poisoned")
        .clone();
    assert2::assert!(
        calls
            .iter()
            .map(|query| (query.query.as_str(), query.shard))
            .collect::<Vec<_>>()
            == vec![
                ("topk(2, up)", Some(QueryShard { index: 1, total: 2 })),
                ("topk(2, up)", Some(QueryShard { index: 2, total: 2 })),
            ]
    );
    let QueryResult::RangeMatrix(series) = result.result else {
        panic!("topk range matrix");
    };
    let selected = series
        .iter()
        .map(|series| {
            let SampleValue::Float(value) = &series.samples[0].value else {
                panic!("topk float sample");
            };
            SelectedTopkValue {
                series: series.labels.get("series").unwrap().to_string(),
                value: *value,
            }
        })
        .collect::<Vec<_>>();
    assert2::assert!(
        selected
            == vec![
                SelectedTopkValue {
                    series: "a".to_string(),
                    value: 10.0
                },
                SelectedTopkValue {
                    series: "c".to_string(),
                    value: 9.0
                },
            ]
    );
}

}

mod frontend_range_execution_retries_only_executor_classified_failures {
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

struct RetryProbeExecutor {
    attempts: AtomicUsize,
    transient_failures: usize,
    retryable: bool,
}

#[async_trait]
impl RangeQueryExecutor for RetryProbeExecutor {
    async fn execute_range_query(
        &self,
        _tenant: &TenantId,
        query: &FrontendRangeQuery,
    ) -> Result<AnnotatedQueryResult, PromqlError> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt <= self.transient_failures {
            return Err(PromqlError::Store("probe failure".into()));
        }
        Ok(unannotated(QueryResult::RangeMatrix(vec![RangeSeries {
            drop_name: false,
            start_timestamps_ms: std::collections::BTreeMap::new(),
            labels: labels(&[crate::test_support::TestLabelPair::new("__name__", "up")]).into(),
            samples: vec![RangeSeriesSample::new(
                query.start_ms,
                SampleValue::Float(1.0),
            )],
        }])))
    }

    fn is_transient_error(&self, _error: &PromqlError) -> bool {
        self.retryable
    }
}

#[tokio::test]
async fn frontend_range_execution_retries_only_executor_classified_failures() {
    let planned = vec![FrontendRangeQuery {
        query: "up".into(),
        start_ms: 0,
        end_ms: 0,
        step: millis(1_000),
        shard: None,
    }];
    let execution_options = krabka_query_frontend::ExecutionOptions {
        max_parallelism: std::num::NonZeroUsize::new(1).expect("one is non-zero"),
        max_retries: 2,
        max_cache_freshness: std::time::Duration::ZERO,
    };

    let transient = RetryProbeExecutor {
        attempts: AtomicUsize::new(0),
        transient_failures: 2,
        retryable: true,
    };
    let cache = QueryFrontendCache::default().with_execution_options(execution_options);
    execute_planned_range_queries(
        &transient,
        &cache,
        &tenant_id("tenant-a"),
        planned.clone(),
        krabka_query_frontend::AdmissionLimits::default(),
    )
    .await
    .unwrap();
    assert2::assert!(transient.attempts.load(Ordering::SeqCst) == 3);

    let permanent = RetryProbeExecutor {
        attempts: AtomicUsize::new(0),
        transient_failures: 2,
        retryable: false,
    };
    let cache = QueryFrontendCache::default().with_execution_options(execution_options);
    let error = execute_planned_range_queries(
        &permanent,
        &cache,
        &tenant_id("tenant-a"),
        planned,
        krabka_query_frontend::AdmissionLimits::default(),
    )
    .await
    .unwrap_err();
    assert2::assert!(matches!(error, PromqlError::Store(_)));
    assert2::assert!(permanent.attempts.load(Ordering::SeqCst) == 1);
}

}

mod frontend_range_execution_runs_against_promql_engine {
use super::*;

#[tokio::test]
pub(crate) async fn frontend_range_execution_runs_against_promql_engine() {
    let store =
        crate::test_support::up_api_store("tenant-a", &[(0, 1.0), (60_000, 2.0), (120_000, 3.0)]);
    let engine = PromqlEngine::new(std::sync::Arc::new(store), EngineOpts::default());
    let cache = QueryFrontendCache::default();

    let result = execute_range_query_frontend(
        &engine,
        &cache,
        &FrontendRangeRequest {
            tenant: tenant_id("tenant-a"),
            query: "up".into(),
            start_ms: 0,
            end_ms: 120_000,
            step: millis(60_000),
            admission_limits: krabka_query_frontend::AdmissionLimits::default(),
            opts: QueryFrontendOptions {
                split_interval: millis(60_000),
                shard_count: 1,
            },
        },
    )
    .await
    .unwrap();

    assert2::assert!(
        result
            == up_api_matrix(&[
                FloatPoint {
                    ts_ms: 0,
                    value: 1.0
                },
                FloatPoint {
                    ts_ms: 60_000,
                    value: 2.0
                },
                FloatPoint {
                    ts_ms: 120_000,
                    value: 3.0
                }
            ])
    );
}

}

mod frontend_range_execution_uses_cache_and_merges_subquery_results {
use super::*;

#[tokio::test]
pub(crate) async fn frontend_range_execution_uses_cache_and_merges_subquery_results() {
    let cache = QueryFrontendCache::default();
    let executor = RecordingExecutor::default();
    let cached_query = up_range_query(60_000, None);
    cache
        .insert(
            "tenant-a",
            &cached_query,
            one_sample_matrix(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ])),
        )
        .await
        .unwrap();

    let result = execute_range_query_frontend(
        &executor,
        &cache,
        &FrontendRangeRequest {
            tenant: tenant_id("tenant-a"),
            query: "up".into(),
            start_ms: 0,
            end_ms: 180_000,
            step: millis(60_000),
            admission_limits: krabka_query_frontend::AdmissionLimits::default(),
            opts: QueryFrontendOptions {
                split_interval: millis(120_000),
                shard_count: 1,
            },
        },
    )
    .await
    .unwrap();

    // Absolute windows [0,120k)->[0,60k] (pre-cached) and
    // [120k,240k)->[120k,180k] (executed fresh).
    let calls = executor
        .calls
        .lock()
        .expect("recording executor calls poisoned")
        .clone();
    assert2::assert!(
        calls
            .iter()
            .map(|query| (query.start_ms, query.end_ms))
            .collect::<Vec<_>>()
            == vec![(120_000, 180_000)]
    );
    assert2::assert!(
        cache
            .get("tenant-a", &calls[0])
            .await
            .unwrap()
            .expect("fresh subquery cached")
            == up_api_matrix(&[FloatPoint {
                ts_ms: 120_000,
                value: 120_000.0
            }])
    );
    assert2::assert!(
        result
            == up_api_matrix(&[
                FloatPoint {
                    ts_ms: 0,
                    value: 1.0
                },
                FloatPoint {
                    ts_ms: 120_000,
                    value: 120_000.0
                }
            ])
    );
}

}
