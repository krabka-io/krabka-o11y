use super::{
    ActiveLogDeleteFilter, CompactionFrontier, FsPath, LabelIndex, MetricEvaluationRange,
    MetricQuery, MetricRangeScanRequest, QueryError, QueryHotTail, StreamPlan, TimeRange, Value,
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
        MetricRangeScanRequest::new(
            plan,
            query,
            label_index,
            MetricEvaluationRange::new(eval_range, step_ns),
            QueryHotTail::<crate::WalLogRecord> {
                records: &[],
                frontier: &CompactionFrontier::new(i64::MAX),
                delete_filters,
            },
        ),
    )
    .await
}
