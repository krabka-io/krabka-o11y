use super::{Arc, MemTable, RecordBatch, Result, SessionContext};

pub(crate) fn register_batches(
    ctx: &SessionContext,
    table_name: &str,
    batches: Vec<RecordBatch>,
) -> Result<()> {
    let schema = batches
        .first()
        .map_or_else(crate::span_columns::span_schema, RecordBatch::schema);
    let table = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table(table_name, Arc::new(table))?;
    Ok(())
}
