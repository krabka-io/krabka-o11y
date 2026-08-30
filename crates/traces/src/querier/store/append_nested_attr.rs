use super::*;

pub(crate) fn append_nested_attr(
    event: Option<&EventRef>,
    link: Option<&LinkRef>,
    attr: NestedAttrColumn<'_>,
    builder: &mut StringBuilder,
) {
    let value = match attr.scope {
        NestedAttrScope::Event => event.and_then(|event| {
            event
                .attributes
                .iter()
                .find(|(key, _)| key == attr.key)
                .map(|(_, value)| value)
        }),
        NestedAttrScope::Link => link.and_then(|link| {
            link.attributes
                .iter()
                .find(|(key, _)| key == attr.key)
                .map(|(_, value)| value)
        }),
    };
    if let Some(value) = value {
        builder.append_value(attr_typed_value_parts(value).1);
    } else {
        builder.append_null();
    }
}
