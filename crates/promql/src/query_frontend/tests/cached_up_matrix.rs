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
