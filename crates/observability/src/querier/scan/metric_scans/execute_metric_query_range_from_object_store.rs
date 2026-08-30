use super::{
    Arc, CompactionFrontier, LabelIndex, MetricQuery, ObjectPath, ObjectStore, QueryError,
    QueryHotTail, StreamPlan, TimeRange, Value,
    execute_metric_query_range_from_object_store_with_hot_tail_frontier,
};

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_metric_query_range_from_object_store(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    eval_range: TimeRange,
    step_ns: i64,
) -> Result<Value, QueryError> {
    execute_metric_query_range_from_object_store_with_hot_tail_frontier(
        store,
        prefix,
        plan,
        query,
        label_index,
        (eval_range, step_ns),
        QueryHotTail {
            records: &[],
            frontier: &CompactionFrontier::new(i64::MAX),
            delete_filters: &[],
        },
    )
    .await
}
