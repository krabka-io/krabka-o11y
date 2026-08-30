use super::*;

pub(crate) async fn collect_table(
    ctx: &SessionContext,
    table: &str,
) -> Result<Vec<RecordBatch>, TraceqlError> {
    Ok(ctx.table(table).await?.collect().await?)
}
