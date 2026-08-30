use super::*;

pub(crate) fn promoted_attr_value<'a>(attrs: &'a [SpanAttr], key: &str) -> Option<&'a AttrValue> {
    attrs
        .iter()
        .find_map(|attr| (attr.key == key && !attr.is_array).then_some(&attr.value))
}
