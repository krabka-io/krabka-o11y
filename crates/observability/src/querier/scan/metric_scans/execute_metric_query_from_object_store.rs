use super::{
    Arc, CompactionFrontier, LabelIndex, MetricQuery, ObjectPath, ObjectStore, QueryError,
    StreamPlan, Value, execute_metric_query_from_object_store_with_hot_tail_frontier,
};

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_metric_query_from_object_store(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
) -> Result<Value, QueryError> {
    execute_metric_query_from_object_store_with_hot_tail_frontier(
        store,
        prefix,
        plan,
        query,
        label_index,
        &[],
        &CompactionFrontier::new(i64::MAX),
    )
    .await
}
