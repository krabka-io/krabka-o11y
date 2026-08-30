use super::{
    ActiveLogDeleteFilter, CompactionFrontier, FsPath, LabelIndex, MetricQuery, QueryError,
    QueryHotTail, StreamPlan, TimeRange, Value, WalLogRecord,
    execute_metric_query_range_with_hot_tail_frontier_and_deletes,
};

pub(crate) async fn execute_metric_query_with_hot_tail_frontier_and_deletes(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, QueryError> {
    let eval_range = TimeRange::new(plan.time_range.end_ns, plan.time_range.end_ns)?;
    execute_metric_query_range_with_hot_tail_frontier_and_deletes(
        root,
        plan,
        query,
        label_index,
        (eval_range, 1),
        QueryHotTail {
            records: hot_tail,
            frontier,
            delete_filters,
        },
    )
    .await
}
