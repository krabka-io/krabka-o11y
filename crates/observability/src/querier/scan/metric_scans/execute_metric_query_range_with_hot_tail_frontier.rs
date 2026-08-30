use super::{
    CompactionFrontier, FsPath, LabelIndex, MetricQuery, QueryError, QueryHotTail, StreamPlan,
    TimeRange, Value, WalLogRecord, execute_metric_query_range_with_hot_tail_frontier_and_deletes,
};

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_metric_query_range_with_hot_tail_frontier(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    evaluation: (TimeRange, i64),
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
) -> Result<Value, QueryError> {
    execute_metric_query_range_with_hot_tail_frontier_and_deletes(
        root,
        plan,
        query,
        label_index,
        evaluation,
        QueryHotTail {
            records: hot_tail,
            frontier,
            delete_filters: &[],
        },
    )
    .await
}
