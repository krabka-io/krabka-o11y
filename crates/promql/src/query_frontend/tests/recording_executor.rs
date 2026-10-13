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
        Ok(unannotated(QueryResult::RangeMatrix(vec![RangeSeries {
            drop_name: false,
            start_timestamps_ms: std::collections::BTreeMap::new(),
            labels: labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ])
            .into(),
            samples: vec![RangeSeriesSample::new(
                query.start_ms,
                SampleValue::Float(120_000.0),
            )],
        }])))
    }
}
