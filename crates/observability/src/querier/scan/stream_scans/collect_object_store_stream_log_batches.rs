use super::*;

pub(crate) async fn collect_object_store_stream_log_batches(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    block: &BlockDescriptor,
    plan: &StreamPlan,
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
        .sql(&stream_plan_scan_sql(plan))
        .await?
        .collect()
        .await?)
}
