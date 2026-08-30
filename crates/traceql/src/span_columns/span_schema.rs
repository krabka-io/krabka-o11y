use super::{SchemaRef, span_schema_with_attrs};

#[must_use]
pub fn span_schema() -> SchemaRef {
    span_schema_with_attrs(&[])
}
