use super::{AttrValue, EventRef, LinkRef, NestedAttrColumn, NestedAttrScope};

pub(crate) fn append_nested_attr(
    event: Option<&EventRef>,
    link: Option<&LinkRef>,
    attr: NestedAttrColumn<'_>,
    builder: &mut Vec<Vec<AttrValue>>,
) {
    let attributes = match attr.scope {
        NestedAttrScope::Event => event.map(|event| &event.attributes),
        NestedAttrScope::Link => link.map(|link| &link.attributes),
    };
    builder.push(
        attributes
            .into_iter()
            .flatten()
            .filter(|(key, _)| key == attr.key)
            .map(|(_, value)| value.clone())
            .collect(),
    );
}
