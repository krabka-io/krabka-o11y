use super::*;

/// An `up` range query from 0 to `end_ms` at a 60s step, for `shard`.
pub(crate) fn up_range_query(end_ms: i64, shard: Option<QueryShard>) -> FrontendRangeQuery {
    FrontendRangeQuery {
        query: "up".into(),
        start_ms: 0,
        end_ms,
        step: millis(60_000),
        shard,
    }
}

/// A one-series matrix result of `series`, holding the float 1 at 0.
pub(crate) fn one_sample_matrix(series: Labels) -> AnnotatedQueryResult {
    unannotated(QueryResult::RangeMatrix(vec![RangeSeries {
        drop_name: false,
        start_timestamps_ms: std::collections::BTreeMap::new(),
        labels: series.into(),
        samples: vec![(0, SampleValue::Float(1.0))],
    }]))
}

/// One float point of an expected matrix series.
#[derive(Clone, Copy)]
pub(crate) struct FloatPoint {
    pub(crate) ts_ms: i64,
    pub(crate) value: f64,
}

/// An unannotated one-series matrix of `up{job="api"}` holding `points`.
pub(crate) fn up_api_matrix(points: &[FloatPoint]) -> AnnotatedQueryResult {
    unannotated(QueryResult::RangeMatrix(vec![RangeSeries {
        drop_name: false,
        start_timestamps_ms: std::collections::BTreeMap::new(),
        labels: labels(&[("__name__", "up"), ("job", "api")]).into(),
        samples: points
            .iter()
            .map(|point| (point.ts_ms, SampleValue::Float(point.value)))
            .collect(),
    }]))
}
