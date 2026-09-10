use super::{DataType, Field, PromotedSpanAttr, PromotedSpanAttrType, SCOL_PROMOTED_ATTR_PREFIX};

/// Recover the promoted-attribute declaration a span-block column was written
/// from.
///
/// The block builder writes one `attr.<key>` column per configured promoted
/// attribute, so a block's own Parquet schema carries the configuration it was
/// written under. Reading it back is what lets the compactor and the querier
/// work on a block whose promoted attributes no longer match today's flags.
///
/// String columns are dictionary-encoded on the write path, but a reader may
/// hand back a plain or view-typed string, so every string spelling maps to
/// the same declaration. A column the write path could not have produced
/// returns `None`.
pub(crate) fn promoted_span_attr_from_field(field: &Field) -> Option<PromotedSpanAttr> {
    let key = field.name().strip_prefix(SCOL_PROMOTED_ATTR_PREFIX)?;
    let value_type = match field.data_type() {
        DataType::Dictionary(_, value) => match value.as_ref() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                PromotedSpanAttrType::String
            }
            _ => return None,
        },
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => PromotedSpanAttrType::String,
        DataType::Int64 => PromotedSpanAttrType::Int,
        DataType::Float64 => PromotedSpanAttrType::Double,
        DataType::Boolean => PromotedSpanAttrType::Bool,
        _ => return None,
    };
    Some(PromotedSpanAttr {
        key: key.to_string(),
        value_type,
    })
}
