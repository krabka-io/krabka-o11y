use super::{
    BlockStore, SchemaRef, TraceqlError, block_err, block_promoted_attrs, span_block_schema,
    span_block_schema_with_promoted_attrs,
};

/// The Arrow schema one span block was written with, read from its Parquet
/// footer.
///
/// A block's promoted attribute columns come from the ingest flags in force
/// when it was written, which need not be the flags in force now, so the block
/// is the only trustworthy source for its own schema. Only the footer is read:
/// the schema names the block's promoted attributes, and the canonical schema
/// is then rebuilt from them so it matches the batches a row-group read
/// decodes.
pub(crate) async fn block_span_schema(
    blocks: &BlockStore,
    object_key: &str,
) -> Result<SchemaRef, TraceqlError> {
    let keys = [object_key.to_string()];
    let (ctx, table) = blocks
        .scan_block_keys(&keys, span_block_schema())
        .await
        .map_err(|err| block_err(&err))?;
    let table = ctx
        .table(&table)
        .await
        .map_err(|err| TraceqlError::Store(err.to_string()))?;
    let promoted_attrs = block_promoted_attrs(table.schema().as_arrow())
        .map_err(|err| TraceqlError::Store(err.to_string()))?;
    Ok(span_block_schema_with_promoted_attrs(&promoted_attrs))
}
