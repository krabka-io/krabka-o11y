use super::*;

#[derive(Default)]
pub(crate) struct RankRecordingExecutor {
    pub(crate) calls: Mutex<Vec<FrontendRangeQuery>>,
}

#[async_trait]
impl RangeQueryExecutor for RankRecordingExecutor {
    async fn execute_range_query(
        &self,
        _tenant: &TenantId,
        query: &FrontendRangeQuery,
    ) -> Result<AnnotatedQueryResult, PromqlError> {
        self.calls
            .lock()
            .expect("rank executor calls poisoned")
            .push(query.clone());

        let shard = query.shard.expect("rank query shard");
        let series = match shard.index {
            1 => vec![
                RangeSeries {
                    drop_name: false,
                    start_timestamps_ms: std::collections::BTreeMap::new(),
                    labels: labels(&[
                        crate::test_support::TestLabelPair::new("__name__", "up"),
                        crate::test_support::TestLabelPair::new("series", "a"),
                    ])
                    .into(),
                    samples: vec![RangeSeriesSample::new(0, SampleValue::Float(10.0))],
                },
                RangeSeries {
                    drop_name: false,
                    start_timestamps_ms: std::collections::BTreeMap::new(),
                    labels: labels(&[
                        crate::test_support::TestLabelPair::new("__name__", "up"),
                        crate::test_support::TestLabelPair::new("series", "b"),
                    ])
                    .into(),
                    samples: vec![RangeSeriesSample::new(0, SampleValue::Float(2.0))],
                },
            ],
            2 => vec![
                RangeSeries {
                    drop_name: false,
                    start_timestamps_ms: std::collections::BTreeMap::new(),
                    labels: labels(&[
                        crate::test_support::TestLabelPair::new("__name__", "up"),
                        crate::test_support::TestLabelPair::new("series", "c"),
                    ])
                    .into(),
                    samples: vec![RangeSeriesSample::new(0, SampleValue::Float(9.0))],
                },
                RangeSeries {
                    drop_name: false,
                    start_timestamps_ms: std::collections::BTreeMap::new(),
                    labels: labels(&[
                        crate::test_support::TestLabelPair::new("__name__", "up"),
                        crate::test_support::TestLabelPair::new("series", "d"),
                    ])
                    .into(),
                    samples: vec![RangeSeriesSample::new(0, SampleValue::Float(8.0))],
                },
            ],
            _ => Vec::new(),
        };
        Ok(unannotated(QueryResult::RangeMatrix(series)))
    }
}
