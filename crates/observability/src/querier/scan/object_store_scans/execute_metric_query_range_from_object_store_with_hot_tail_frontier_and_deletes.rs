use std::borrow::Borrow;

use super::{
    super::{
        MetricRangeScanRequest, MetricRangeScanSource, QueryError, Value,
        execute_metric_range_scan_with_deletes,
    },
    ColdBlockScan,
};
use crate::WalLogRecord;

pub(crate) async fn execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes<
    R: Borrow<WalLogRecord> + Sync,
>(
    cold: ColdBlockScan<'_>,
    request: MetricRangeScanRequest<'_, R>,
) -> Result<Value, QueryError> {
    execute_metric_range_scan_with_deletes(MetricRangeScanSource::ObjectStore(cold), request).await
}
