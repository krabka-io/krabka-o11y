use std::borrow::Borrow;

use super::{
    super::registered_local_log_session, BTreeMap, FsPath, HotTailMetricSamples,
    MetricRangeScanRequest, PreparedMetricRangeScan, QueryError, Value, loki_matrix_response,
    metric_plan_scan_sql, metric_samples_from_batches,
};
use crate::WalLogRecord;

pub(crate) async fn execute_metric_query_range_with_hot_tail_frontier_and_deletes<
    R: Borrow<WalLogRecord> + Sync,
>(
    root: impl AsRef<FsPath>,
    request: MetricRangeScanRequest<'_, R>,
) -> Result<Value, QueryError> {
    let PreparedMetricRangeScan {
        plan,
        query,
        label_index,
        eval_range,
        eval_times,
        hot_tail,
    } = request.prepare()?;
    let mut samples = BTreeMap::new();

    if !plan.blocks.is_empty() && !plan.fingerprints.is_empty() {
        let ctx = registered_local_log_session(root, &plan.blocks)?;
        let sql = metric_plan_scan_sql(plan, query, eval_range)?;
        let batches = ctx.sql(&sql).await?.collect().await?;
        samples = metric_samples_from_batches(
            &batches,
            plan,
            query,
            label_index,
            &eval_times,
            hot_tail.delete_filters,
        )?;
    }

    let series = HotTailMetricSamples {
        plan,
        query,
        eval_times: &eval_times,
        hot_tail,
    }
    .merge_into(samples)?;

    Ok(loki_matrix_response(series))
}
