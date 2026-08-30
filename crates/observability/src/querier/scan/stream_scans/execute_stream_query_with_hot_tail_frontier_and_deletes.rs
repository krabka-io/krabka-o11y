use super::{
    ActiveLogDeleteFilter, BTreeMap, CompactionFrontier, FsPath, LabelIndex, Labels, QueryError,
    SessionContext, StreamPlan, Value, WalLogRecord, append_matching_hot_log_record,
    append_matching_log_batches, loki_streams_response, register_log_blocks,
    sort_loki_stream_values, stream_plan_scan_sql,
};

pub(crate) async fn execute_stream_query_with_hot_tail_frontier_and_deletes(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, QueryError> {
    let mut streams: BTreeMap<Labels, Vec<[String; 2]>> = BTreeMap::new();

    if !plan.blocks.is_empty() && !plan.fingerprints.is_empty() {
        let ctx = SessionContext::new();
        register_log_blocks(&ctx, "logs", root, &plan.blocks)?;
        let sql = stream_plan_scan_sql(plan);
        let batches = ctx.sql(&sql).await?.collect().await?;
        append_matching_log_batches(&mut streams, plan, label_index, &batches, delete_filters)?;
    }

    for record in hot_tail {
        append_matching_hot_log_record(&mut streams, plan, record, frontier, delete_filters);
    }
    sort_loki_stream_values(&mut streams);

    Ok(loki_streams_response(streams))
}
