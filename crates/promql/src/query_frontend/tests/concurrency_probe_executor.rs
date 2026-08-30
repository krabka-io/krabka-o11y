use super::*;

/// Executor that blocks every sub-query on a shared barrier. The barrier size is
/// the expected fan-out width. A sequential dispatcher can never satisfy the
/// barrier, because only one sub-query is ever in flight, so the surrounding
/// `tokio::time::timeout` trips. A concurrent dispatcher releases all N at once.
/// The executor also records the wall-clock order in which it admitted the
/// sub-queries, which proves that it dispatched every planned sub-query.
pub(crate) struct ConcurrencyProbeExecutor {
    pub(crate) barrier: tokio::sync::Barrier,
    pub(crate) calls: Mutex<Vec<FrontendRangeQuery>>,
}

impl ConcurrencyProbeExecutor {
    pub(crate) fn new(width: usize) -> Self {
        Self {
            barrier: tokio::sync::Barrier::new(width),
            calls: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl RangeQueryExecutor for ConcurrencyProbeExecutor {
    async fn execute_range_query(
        &self,
        _tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<QueryResult, PromqlError> {
        self.calls
            .lock()
            .expect("probe executor calls poisoned")
            .push(query.clone());
        // All concurrently-dispatched sub-queries must reach here before any
        // can proceed. Under sequential dispatch this never completes.
        self.barrier.wait().await;
        // Each sub-query contributes a sample at a distinct timestamp
        // (its split start), so the stitched matrix is order-independent.
        Ok(QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[("__name__", "up"), ("job", "api")]),
            samples: vec![(query.start_ms, SampleValue::Float(1.0))],
        }]))
    }
}
