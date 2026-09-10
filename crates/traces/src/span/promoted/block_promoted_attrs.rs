use super::{
    PromotedSpanAttr, Schema, TracesError, promoted_span_attr_from_field, span_block_schema,
};

/// The promoted attributes a span block was written with, read from the
/// block's own schema.
///
/// A block's schema is the base span schema with one `attr.<key>` column per
/// promoted attribute appended, so every column that is not a base column must
/// be a promoted one. A column that is neither is a block this build cannot
/// interpret, and silently ignoring it would drop data on compaction.
pub(crate) fn block_promoted_attrs(schema: &Schema) -> Result<Vec<PromotedSpanAttr>, TracesError> {
    let base = span_block_schema();
    let mut attrs = Vec::new();
    for field in schema.fields() {
        if base.column_with_name(field.name()).is_some() {
            continue;
        }
        let attr = promoted_span_attr_from_field(field).ok_or_else(|| {
            TracesError::Block(format!(
                "span block column `{}` of type {} is neither a base column nor a \
                 promoted attribute column",
                field.name(),
                field.data_type()
            ))
        })?;
        attrs.push(attr);
    }
    Ok(attrs)
}
