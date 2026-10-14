use std::borrow::Borrow;

use super::{
    ColdBlockScan, LabelIndex, MetricEvaluationRange, MetricQuery, MetricRangeScanRequest,
    QueryError, QueryHotTail, StreamPlan, TimeRange, Value,
    execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes,
};
use crate::WalLogRecord;

pub(crate) async fn execute_metric_query_from_object_store_with_hot_tail_frontier_and_deletes<
    R: Borrow<WalLogRecord> + Sync,
>(
    cold: ColdBlockScan<'_>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    hot_tail: QueryHotTail<'_, R>,
) -> Result<Value, QueryError> {
    let eval_range = TimeRange::new(plan.time_range.end_ns, plan.time_range.end_ns)?;
    execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes(
        cold,
        MetricRangeScanRequest::new(
            plan,
            query,
            label_index,
            MetricEvaluationRange::new(eval_range, 1),
            hot_tail,
        ),
    )
    .await
}
