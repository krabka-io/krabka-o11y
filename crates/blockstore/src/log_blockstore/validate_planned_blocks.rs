use super::*;

pub(crate) fn validate_planned_blocks(blocks: &[BlockDescriptor]) -> Result<(), BlockStoreError> {
    if blocks.is_empty() {
        return Err(BlockStoreError::EmptyBlockScan);
    }
    Ok(())
}
