use super::{
    FsPath, LabelIndex, MetricQuery, QueryError, StreamPlan, TimeRange, Value,
    execute_metric_query_range_with_deletes,
};

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_metric_query_range(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    eval_range: TimeRange,
    step_ns: i64,
) -> Result<Value, QueryError> {
    execute_metric_query_range_with_deletes(
        root,
        plan,
        query,
        label_index,
        eval_range,
        step_ns,
        &[],
    )
    .await
}
