use std::borrow::Borrow;

use super::{
    ColdBlockScan, LabelIndex, MetricQuery, QueryError, QueryHotTail, StreamPlan, TimeRange, Value,
    execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes,
};
use crate::WalLogRecord;

pub(crate) async fn execute_metric_query_range_from_object_store_with_hot_tail_frontier<
    R: Borrow<WalLogRecord> + Sync,
>(
    cold: ColdBlockScan<'_>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    evaluation: (TimeRange, i64),
    hot_tail: QueryHotTail<'_, R>,
) -> Result<Value, QueryError> {
    execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes(
        cold,
        plan,
        query,
        label_index,
        evaluation,
        hot_tail,
    )
    .await
}
