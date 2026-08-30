use super::{
    Arc, BlockDescriptor, MetricQuery, ObjectPath, ObjectStore, QueryError, RecordBatch,
    SessionContext, StreamPlan, TimeRange, metric_plan_scan_sql,
    register_log_blocks_from_object_store,
};

pub(crate) async fn collect_object_store_metric_log_batches(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    block: &BlockDescriptor,
    plan: &StreamPlan,
    query: &MetricQuery,
    eval_range: TimeRange,
) -> Result<Vec<RecordBatch>, QueryError> {
    let ctx = SessionContext::new();
    register_log_blocks_from_object_store(
        &ctx,
        "logs",
        store,
        prefix.clone(),
        std::slice::from_ref(block),
    )?;
    Ok(ctx
        .sql(&metric_plan_scan_sql(plan, query, eval_range)?)
        .await?
        .collect()
        .await?)
}
