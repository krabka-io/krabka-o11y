use super::*;

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_metric_query_range_with_hot_tail(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    evaluation: (TimeRange, i64),
    hot_tail: &[WalLogRecord],
    compacted_through_ns: i64,
) -> Result<Value, QueryError> {
    execute_metric_query_range_with_hot_tail_frontier(
        root,
        plan,
        query,
        label_index,
        evaluation,
        hot_tail,
        &CompactionFrontier::new(compacted_through_ns),
    )
    .await
}
