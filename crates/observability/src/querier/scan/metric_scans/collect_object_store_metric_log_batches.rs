use super::{
    Arc, BlockDescriptor, MetricQuery, ObjectPath, ObjectStore, QueryError, RecordBatch,
    StreamPlan, TimeRange, metric_plan_scan_sql,
};

pub(crate) async fn collect_object_store_metric_log_batches(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    block: &BlockDescriptor,
    plan: &StreamPlan,
    query: &MetricQuery,
    eval_range: TimeRange,
) -> Result<Vec<RecordBatch>, QueryError> {
    super::super::collect_object_store_log_batches(
        store,
        prefix,
        block,
        &metric_plan_scan_sql(plan, query, eval_range)?,
    )
    .await
}
