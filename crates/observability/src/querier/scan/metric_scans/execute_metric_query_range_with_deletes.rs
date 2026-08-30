use super::{
    ActiveLogDeleteFilter, CompactionFrontier, FsPath, LabelIndex, MetricQuery, QueryError,
    QueryHotTail, StreamPlan, TimeRange, Value,
    execute_metric_query_range_with_hot_tail_frontier_and_deletes,
};

pub(crate) async fn execute_metric_query_range_with_deletes(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    eval_range: TimeRange,
    step_ns: i64,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, QueryError> {
    execute_metric_query_range_with_hot_tail_frontier_and_deletes(
        root,
        plan,
        query,
        label_index,
        (eval_range, step_ns),
        QueryHotTail {
            records: &[],
            frontier: &CompactionFrontier::new(i64::MAX),
            delete_filters,
        },
    )
    .await
}
