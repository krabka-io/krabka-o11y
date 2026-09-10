use super::{BlockStoreError, RecordBatch, Result, SchemaRef};

/// Rejects a batch whose schema is not the one the block is being written
/// with.
///
/// `position` names the batch in the error, counting from the first batch the
/// block received: the buffered writer's index into its slice, and the
/// streaming writer's count of the batches already handed over.
///
/// # Errors
/// Returns [`BlockStoreError::InvalidBlock`] when the schemas differ.
pub(crate) fn validate_batch_schema(
    schema: &SchemaRef,
    batch: &RecordBatch,
    position: usize,
) -> Result<()> {
    if batch.schema().as_ref() == schema.as_ref() {
        return Ok(());
    }
    Err(BlockStoreError::InvalidBlock(format!(
        "batch {position} schema does not match writer schema"
    )))
}
