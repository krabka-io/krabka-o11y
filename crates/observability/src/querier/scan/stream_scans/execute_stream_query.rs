use super::{FsPath, LabelIndex, QueryError, StreamPlan, Value, execute_stream_query_with_deletes};

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_stream_query(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
) -> Result<Value, QueryError> {
    execute_stream_query_with_deletes(root, plan, label_index, &[]).await
}
