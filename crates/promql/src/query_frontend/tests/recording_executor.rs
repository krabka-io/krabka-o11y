use super::*;

#[derive(Default)]
pub(crate) struct RecordingExecutor {
    pub(crate) calls: Mutex<Vec<FrontendRangeQuery>>,
}

#[async_trait]
impl RangeQueryExecutor for RecordingExecutor {
    async fn execute_range_query(
        &self,
        _tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<QueryResult, PromqlError> {
        self.calls
            .lock()
            .expect("recording executor calls poisoned")
            .push(query.clone());
        Ok(QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[("__name__", "up"), ("job", "api")]),
            samples: vec![(query.start_ms, SampleValue::Float(120_000.0))],
        }]))
    }
}
