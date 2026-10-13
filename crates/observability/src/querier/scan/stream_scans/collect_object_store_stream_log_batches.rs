use super::{
    Arc, BlockDescriptor, ObjectPath, ObjectStore, QueryError, RecordBatch, StreamPlan,
    stream_plan_scan_sql,
};

pub(crate) async fn collect_object_store_stream_log_batches(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    block: &BlockDescriptor,
    plan: &StreamPlan,
) -> Result<Vec<RecordBatch>, QueryError> {
    super::super::collect_object_store_log_batches(
        store,
        prefix,
        block,
        &stream_plan_scan_sql(plan),
    )
    .await
}
