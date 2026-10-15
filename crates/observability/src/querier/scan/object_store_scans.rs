use std::borrow::Borrow;
pub(crate) use super::{MetricEvaluationRange, MetricRangeScanRequest};
use super::{execute_metric_range_scan_with_deletes, MetricRangeScanSource};
use crate::{
    Arc, LabelIndex, MetricQuery, NonZeroUsize, ObjectPath, ObjectStore, QueryError, QueryHotTail,
    StreamPlan, Value, WalLogRecord,
};

mod cold_block_scan;

pub(crate) use cold_block_scan::ColdBlockScan;

pub(crate) async fn execute_metric_query_range_from_object_store_with_hot_tail_frontier<
    R: Borrow<WalLogRecord> + Sync,
>(
    cold: ColdBlockScan<'_>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    evaluation: MetricEvaluationRange,
    hot_tail: QueryHotTail<'_, R>,
) -> Result<Value, QueryError> {
    execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes(
        cold,
        MetricRangeScanRequest::new(plan, query, label_index, evaluation, hot_tail),
    )
    .await
}

pub(crate) async fn execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes<
    R: Borrow<WalLogRecord> + Sync,
>(
    cold: ColdBlockScan<'_>,
    request: MetricRangeScanRequest<'_, R>,
) -> Result<Value, QueryError> {
    execute_metric_range_scan_with_deletes(MetricRangeScanSource::ObjectStore(cold), request).await
}
