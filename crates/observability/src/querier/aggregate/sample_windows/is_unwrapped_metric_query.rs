use super::{MetricQuery, PipelineStage};

pub(crate) fn is_unwrapped_metric_query(query: &MetricQuery) -> bool {
    query
        .stream
        .pipeline
        .iter()
        .any(|stage| matches!(stage, PipelineStage::Unwrap(_)))
}
