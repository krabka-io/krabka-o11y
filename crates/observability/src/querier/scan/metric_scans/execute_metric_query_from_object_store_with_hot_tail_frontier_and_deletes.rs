use super::{
    Arc, LabelIndex, MetricQuery, ObjectPath, ObjectStore, QueryError, QueryHotTail, StreamPlan,
    TimeRange, Value,
    execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes,
};

pub(crate) async fn execute_metric_query_from_object_store_with_hot_tail_frontier_and_deletes(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    hot_tail: QueryHotTail<'_>,
) -> Result<Value, QueryError> {
    let eval_range = TimeRange::new(plan.time_range.end_ns, plan.time_range.end_ns)?;
    execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes(
        store,
        prefix,
        plan,
        query,
        label_index,
        (eval_range, 1),
        hot_tail,
    )
    .await
}
