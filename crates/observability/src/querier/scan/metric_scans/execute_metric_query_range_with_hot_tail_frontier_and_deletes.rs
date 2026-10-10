use std::borrow::Borrow;

use super::{
    BTreeMap, FsPath, HotTailMetricSamples, LabelIndex, MetricQuery, QueryError, QueryHotTail,
    SessionContext, StreamPlan, TimeRange, Value, checked_eval_times, loki_matrix_response,
    metric_plan_scan_sql, metric_samples_from_batches, register_log_blocks,
};
use crate::WalLogRecord;

pub(crate) async fn execute_metric_query_range_with_hot_tail_frontier_and_deletes<
    R: Borrow<WalLogRecord> + Sync,
>(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    evaluation: (TimeRange, i64),
    hot_tail: QueryHotTail<'_, R>,
) -> Result<Value, QueryError> {
    let (eval_range, step_ns) = evaluation;
    let eval_times = checked_eval_times(eval_range, step_ns)?;
    let mut samples = BTreeMap::new();

    if !plan.blocks.is_empty() && !plan.fingerprints.is_empty() {
        let ctx = SessionContext::new();
        register_log_blocks(&ctx, "logs", root, &plan.blocks)?;
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
