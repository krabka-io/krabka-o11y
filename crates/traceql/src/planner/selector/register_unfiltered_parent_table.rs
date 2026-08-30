use super::{
    Arc, MemTable, PlannerContext, RecordBatch, Result, SessionContext, SpanStore, collect_table,
};

pub(crate) async fn register_unfiltered_parent_table<S: SpanStore>(
    store: &S,
    ctx: &PlannerContext,
    target_ctx: &SessionContext,
) -> Result<String> {
    let parent_scan = store
        .scan_with_options(
            &ctx.tenant,
            &[],
            ctx.start_ns.into(),
            ctx.end_ns.into(),
            &ctx.scan_options,
        )
        .await?;
    let batches = collect_table(&parent_scan.ctx, &parent_scan.span_table).await?;
    let schema = batches
        .first()
        .map_or_else(crate::span_columns::span_schema, RecordBatch::schema);
    let table = MemTable::try_new(schema, vec![batches])?;
    let table_name = "parent_spans";
    target_ctx.register_table(table_name, Arc::new(table))?;
    Ok(table_name.to_string())
}
