use super::*;

pub(crate) async fn execute_stream_query_with_deletes(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, QueryError> {
    execute_stream_query_with_hot_tail_frontier_and_deletes(
        root,
        plan,
        label_index,
        &[],
        &CompactionFrontier::new(i64::MAX),
        delete_filters,
    )
    .await
}
