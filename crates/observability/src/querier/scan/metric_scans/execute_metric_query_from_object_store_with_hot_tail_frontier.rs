use super::{
    ColdBlockScan, CompactionFrontier, LabelIndex, MetricQuery, QueryError, QueryHotTail,
    StreamPlan, Value, WalLogRecord,
    execute_metric_query_from_object_store_with_hot_tail_frontier_and_deletes,
};

pub(crate) async fn execute_metric_query_from_object_store_with_hot_tail_frontier(
    cold: ColdBlockScan<'_>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
) -> Result<Value, QueryError> {
    execute_metric_query_from_object_store_with_hot_tail_frontier_and_deletes(
        cold,
        plan,
        query,
        label_index,
        QueryHotTail {
            records: hot_tail,
            frontier,
            delete_filters: &[],
        },
    )
    .await
}
