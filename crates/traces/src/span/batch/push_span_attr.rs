use super::{SpanAttr, AttrValue, block_attr_value, same_block_attr_type, extend_block_attr_value};

pub(crate) fn push_span_attr(attrs: &mut Vec<SpanAttr>, key: String, value: &AttrValue) {
    let value = block_attr_value(value);
    if let Some(existing) = attrs
        .iter_mut()
        .find(|attr| attr.key == key && same_block_attr_type(&attr.value, &value))
    {
        extend_block_attr_value(&mut existing.value, value);
        existing.is_array = true;
        return;
    }
    attrs.push(SpanAttr {
        key,
        is_array: false,
        value,
    });
}
