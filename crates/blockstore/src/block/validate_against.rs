use super::*;

/// Validates an Arrow schema against a declared signal block schema.
///
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn validate_against(schema: &Schema, decl: &crate::block_index::BlockSchema) -> Result<()> {
    for col in &decl.required {
        let found = schema.column_with_name(&col.name).ok_or_else(|| {
            BlockStoreError::InvalidBlock(format!("missing `{}` column", col.name))
        })?;
        if found.1.data_type() != &col.data_type {
            return Err(BlockStoreError::InvalidBlock(format!(
                "`{}` must be {:?}, got {:?}",
                col.name,
                col.data_type,
                found.1.data_type()
            )));
        }
        if !col.nullable && found.1.is_nullable() {
            return Err(BlockStoreError::InvalidBlock(format!(
                "`{}` must be non-nullable",
                col.name
            )));
        }
    }
    Ok(())
}
