use super::{AttrValue, attr_value_display};

pub(crate) fn push_scoped_attr(
    attrs: &mut Vec<(String, String)>,
    scope: &str,
    key: &str,
    value: &AttrValue,
) {
    attrs.push((format!("{scope}.{key}"), attr_value_display(value)));
}
