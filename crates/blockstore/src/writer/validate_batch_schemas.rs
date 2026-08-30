use super::*;

pub(crate) fn validate_batch_schemas(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<()> {
    for (index, batch) in batches.iter().enumerate() {
        if batch.schema().as_ref() != schema.as_ref() {
            return Err(BlockStoreError::InvalidBlock(format!(
                "batch {index} schema does not match writer schema"
            )));
        }
    }
    Ok(())
}
