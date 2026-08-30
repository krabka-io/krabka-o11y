use super::{Result, Schema, validate_against};

/// Validates that an Arrow schema carries the mandatory columns with the
/// required types. Payload columns are unconstrained.
///
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn validate_block_schema(schema: &Schema) -> Result<()> {
    validate_against(schema, &crate::block_index::series_block_schema())
}
