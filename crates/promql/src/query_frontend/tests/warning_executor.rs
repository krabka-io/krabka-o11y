use super::*;

// Executor that raises one warning and one info per sub-query, and counts the
// sub-queries it ran. The count separates a cache hit from a fresh evaluation,
// so a test can prove that a hit reports the annotations of the miss that
// populated it rather than re-running the executor.
#[derive(Default)]
pub(crate) struct WarningExecutor {
    pub(crate) calls: Mutex<usize>,
}

impl WarningExecutor {
    pub(crate) fn call_count(&self) -> usize {
        *self.calls.lock().expect("warning executor calls poisoned")
    }
}

#[async_trait]
impl RangeQueryExecutor for WarningExecutor {
    async fn execute_range_query(
        &self,
        _tenant: &TenantId,
        query: &FrontendRangeQuery,
    ) -> Result<AnnotatedQueryResult, PromqlError> {
        *self.calls.lock().expect("warning executor calls poisoned") += 1;
        let mut annotations = Annotations::new();
        annotations.warn("PromQL warning: block metrics/float/0001.parquet is missing");
        annotations.info("PromQL info: metric might not be a counter");
        Ok(AnnotatedQueryResult {
            result: QueryResult::RangeMatrix(vec![RangeSeries {
                labels: labels(&[("__name__", "up"), ("job", "api")]),
                samples: vec![(query.start_ms, SampleValue::Float(1.0))],
            }]),
            annotations,
        })
    }
}
