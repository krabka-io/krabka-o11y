use super::*;

#[derive(Default)]
pub(crate) struct RecordingExecutor {
    pub(crate) calls: Mutex<Vec<FrontendRangeQuery>>,
}

#[async_trait]
impl RangeQueryExecutor for RecordingExecutor {
    async fn execute_range_query(
        &self,
        _tenant: &TenantId,
        query: &FrontendRangeQuery,
    ) -> Result<AnnotatedQueryResult, PromqlError> {
        self.calls
            .lock()
            .expect("recording executor calls poisoned")
            .push(query.clone());
        Ok(up_api_matrix(&[FloatPoint {
            ts_ms: query.start_ms,
            value: 120_000.0,
        }]))
    }
}
