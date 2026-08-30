use super::{
    CompactionFrontier, FsPath, LabelIndex, QueryError, StreamPlan, Value, WalLogRecord,
    execute_stream_query_with_hot_tail_frontier_and_deletes,
};

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_stream_query_with_hot_tail_frontier(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
) -> Result<Value, QueryError> {
    execute_stream_query_with_hot_tail_frontier_and_deletes(
        root,
        plan,
        label_index,
        hot_tail,
        frontier,
        &[],
    )
    .await
}
