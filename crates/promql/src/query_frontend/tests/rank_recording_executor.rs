use super::*;

pub(super) fn ranked_series(name: &str, value: f64) -> RangeSeries {
    RangeSeries {
        drop_name: false,
        start_timestamps_ms: std::collections::BTreeMap::new(),
        labels: labels(&[
            crate::test_support::TestLabelPair::new("__name__", "up"),
            crate::test_support::TestLabelPair::new("series", name),
        ])
        .into(),
        samples: vec![RangeSeriesSample::new(0, SampleValue::Float(value))],
    }
}

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
            1 => vec![ranked_series("a", 10.0), ranked_series("b", 2.0)],
            2 => vec![ranked_series("c", 9.0), ranked_series("d", 8.0)],
            _ => Vec::new(),
        };
        Ok(unannotated(QueryResult::RangeMatrix(series)))
    }
}
