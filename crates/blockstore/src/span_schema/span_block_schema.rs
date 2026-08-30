use super::{SchemaRef, span_block_schema_with_promoted_attrs};

/// The flattened span-per-row Arrow schema.
#[must_use]
pub fn span_block_schema() -> SchemaRef {
    span_block_schema_with_promoted_attrs(&[])
}
