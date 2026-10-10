use super::*;

pub(crate) fn unannotated(result: QueryResult) -> AnnotatedQueryResult {
    AnnotatedQueryResult {
        result,
        annotations: Annotations::new(),
    }
}

/// An unannotated one-series matrix of an unlabeled `partial` at the start of
/// `query`, as a sharded partial aggregate answers.
pub(crate) fn unlabeled_partial(query: &FrontendRangeQuery, partial: f64) -> AnnotatedQueryResult {
    unannotated(QueryResult::RangeMatrix(vec![RangeSeries {
        drop_name: false,
        start_timestamps_ms: std::collections::BTreeMap::new(),
        labels: labels(&[]).into(),
        samples: vec![(query.start_ms, SampleValue::Float(partial))],
    }]))
}
