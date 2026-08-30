use super::*;

pub(crate) async fn execute_metric_query_range_from_object_store_with_hot_tail_frontier(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    evaluation: (TimeRange, i64),
    hot_tail: QueryHotTail<'_>,
) -> Result<Value, QueryError> {
    execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes(
        store,
        prefix,
        plan,
        query,
        label_index,
        evaluation,
        hot_tail,
    )
    .await
}
