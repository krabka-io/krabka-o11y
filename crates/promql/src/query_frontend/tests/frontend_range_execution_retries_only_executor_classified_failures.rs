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
            labels: labels(&[("__name__", "up")]),
            samples: vec![(query.start_ms, SampleValue::Float(1.0))],
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
    execute_planned_range_queries(&transient, &cache, &tenant_id("tenant-a"), planned.clone())
        .await
        .unwrap();
    assert2::assert!(transient.attempts.load(Ordering::SeqCst) == 3);

    let permanent = RetryProbeExecutor {
        attempts: AtomicUsize::new(0),
        transient_failures: 2,
        retryable: false,
    };
    let cache = QueryFrontendCache::default().with_execution_options(execution_options);
    let error = execute_planned_range_queries(&permanent, &cache, &tenant_id("tenant-a"), planned)
        .await
        .unwrap_err();
    assert2::assert!(matches!(error, PromqlError::Store(_)));
    assert2::assert!(permanent.attempts.load(Ordering::SeqCst) == 1);
}
