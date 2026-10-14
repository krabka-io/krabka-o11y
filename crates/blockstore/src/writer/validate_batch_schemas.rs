use super::{RecordBatch, Result, SchemaRef, validate_batch_schema};

/// Rejects a batch set that does not all carry the block's schema.
///
/// # Errors
/// Returns [`BlockStoreError::InvalidBlock`](crate::BlockStoreError::InvalidBlock)
/// naming the first batch that differs.
pub(crate) fn validate_batch_schemas(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<()> {
    for (index, batch) in batches.iter().enumerate() {
        validate_batch_schema(schema, batch, index)?;
    }
    Ok(())
}
