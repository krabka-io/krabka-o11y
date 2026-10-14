use std::borrow::Borrow;

use super::super::{
    MetricRangeScanRequest, MetricRangeScanSource, QueryError, Value,
    execute_metric_range_scan_with_deletes,
};
use crate::WalLogRecord;

pub(crate) async fn execute_metric_query_range_with_hot_tail_frontier_and_deletes<
    R: Borrow<WalLogRecord> + Sync,
>(
    root: impl AsRef<std::path::Path>,
    request: MetricRangeScanRequest<'_, R>,
) -> Result<Value, QueryError> {
    execute_metric_range_scan_with_deletes(MetricRangeScanSource::Local(root.as_ref()), request)
        .await
}
