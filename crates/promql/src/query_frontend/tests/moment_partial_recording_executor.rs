use super::*;

#[derive(Default)]
pub(crate) struct MomentPartialRecordingExecutor {
    pub(crate) calls: Mutex<Vec<FrontendRangeQuery>>,
}

#[async_trait]
impl RangeQueryExecutor for MomentPartialRecordingExecutor {
    async fn execute_range_query(
        &self,
        _tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<QueryResult, PromqlError> {
        self.calls
            .lock()
            .expect("moment partial executor calls poisoned")
            .push(query.clone());

        let shard = query.shard.expect("moment partial query shard");
        let value = match (query.query.as_str(), shard.index) {
            ("sum(up)", 1) => 12.0,
            ("sum(up)", 2) => 3.0,
            ("count(up)", 1) => 2.0,
            ("count(up)", 2) => 1.0,
            ("sum((up) * (up))", 1) => 104.0,
            ("sum((up) * (up))", 2) => 9.0,
            _ => {
                return Err(PromqlError::Plan(format!(
                    "unexpected moment partial query: {query:?}"
                )));
            }
        };
        Ok(QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[]),
            samples: vec![(query.start_ms, SampleValue::Float(value))],
        }]))
    }
}
