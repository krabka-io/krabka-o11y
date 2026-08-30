use super::*;

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_stream_query_from_object_store(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    label_index: &LabelIndex,
) -> Result<Value, QueryError> {
    execute_stream_query_from_object_store_with_hot_tail_frontier(
        store,
        prefix,
        plan,
        label_index,
        QueryHotTail {
            records: &[],
            frontier: &CompactionFrontier::new(i64::MAX),
            delete_filters: &[],
        },
    )
    .await
}
