use super::*;

#[derive(Default)]
pub(crate) struct AvgPartialRecordingExecutor {
    pub(crate) calls: Mutex<Vec<FrontendRangeQuery>>,
}

#[async_trait]
impl RangeQueryExecutor for AvgPartialRecordingExecutor {
    async fn execute_range_query(
        &self,
        _tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<QueryResult, PromqlError> {
        self.calls
            .lock()
            .expect("avg partial executor calls poisoned")
            .push(query.clone());

        let shard = query.shard.expect("avg partial query shard");
        let value = match (query.query.as_str(), shard.index) {
            ("sum(up)", 1) | ("count(up)", 2) => 2.0,
            ("sum(up)", 2) => 10.0,
            ("count(up)", 1) => 1.0,
            _ => {
                return Err(PromqlError::Plan(format!(
                    "unexpected avg partial query: {query:?}"
                )));
            }
        };
        Ok(QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[]),
            samples: vec![(query.start_ms, SampleValue::Float(value))],
        }]))
    }
}
