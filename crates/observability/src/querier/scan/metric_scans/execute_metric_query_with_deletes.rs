use super::*;

pub(crate) async fn execute_metric_query_with_deletes(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, QueryError> {
    let eval_range = TimeRange::new(plan.time_range.end_ns, plan.time_range.end_ns)?;
    execute_metric_query_range_with_deletes(
        root,
        plan,
        query,
        label_index,
        eval_range,
        1,
        delete_filters,
    )
    .await
}
