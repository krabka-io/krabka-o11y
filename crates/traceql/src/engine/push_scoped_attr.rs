use super::{AttrValue, DisplayAttribute, attr_value_display};

pub(crate) fn push_scoped_attr(
    attrs: &mut Vec<DisplayAttribute>,
    scope: &str,
    key: &str,
    value: &AttrValue,
) {
    attrs.push(DisplayAttribute::new(
        format!("{scope}.{key}"),
        attr_value_display(value),
    ));
}
