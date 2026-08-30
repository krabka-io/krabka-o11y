use super::*;

#[derive(Default)]
pub(crate) struct RankRecordingExecutor {
    pub(crate) calls: Mutex<Vec<FrontendRangeQuery>>,
}

#[async_trait]
impl RangeQueryExecutor for RankRecordingExecutor {
    async fn execute_range_query(
        &self,
        _tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<QueryResult, PromqlError> {
        self.calls
            .lock()
            .expect("rank executor calls poisoned")
            .push(query.clone());

        let shard = query.shard.expect("rank query shard");
        let series = match shard.index {
            1 => vec![
                RangeSeries {
                    labels: labels(&[("__name__", "up"), ("series", "a")]),
                    samples: vec![(0, SampleValue::Float(10.0))],
                },
                RangeSeries {
                    labels: labels(&[("__name__", "up"), ("series", "b")]),
                    samples: vec![(0, SampleValue::Float(2.0))],
                },
            ],
            2 => vec![
                RangeSeries {
                    labels: labels(&[("__name__", "up"), ("series", "c")]),
                    samples: vec![(0, SampleValue::Float(9.0))],
                },
                RangeSeries {
                    labels: labels(&[("__name__", "up"), ("series", "d")]),
                    samples: vec![(0, SampleValue::Float(8.0))],
                },
            ],
            _ => Vec::new(),
        };
        Ok(QueryResult::RangeMatrix(series))
    }
}
