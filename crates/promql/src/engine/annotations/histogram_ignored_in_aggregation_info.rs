/// Exact Prometheus `HistogramIgnoredInAggregationInfo` text for `aggregation`.
///
/// `aggregation` is the operator's own name: `min`, `max`, `stddev`, `stdvar`,
/// `quantile`, `topk`, or `bottomk`.
pub(crate) fn histogram_ignored_in_aggregation_info(aggregation: &str) -> String {
    format!("PromQL info: ignored histogram in {aggregation} aggregation")
}
