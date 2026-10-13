use super::{AttrValue, EVENT_ATTR_PREFIX, EventRef, InputSpan, LINK_ATTR_PREFIX, LinkRef};

pub(crate) fn nested_attr_value<'a>(
    key: &str,
    span: &'a InputSpan,
    event: Option<&'a EventRef>,
    link: Option<&'a LinkRef>,
) -> Option<&'a AttrValue> {
    if let Some(key) = key.strip_prefix(EVENT_ATTR_PREFIX) {
        return event.and_then(|event| {
            event
                .attributes
                .iter()
                .find(|attribute| attribute.key == key)
                .map(|attribute| &attribute.value)
        });
    }
    if let Some(key) = key.strip_prefix(LINK_ATTR_PREFIX) {
        return link.and_then(|link| {
            link.attributes
                .iter()
                .find(|attribute| attribute.key == key)
                .map(|attribute| &attribute.value)
        });
    }
    span.attrs
        .iter()
        .find(|attribute| attribute.key == key)
        .map(|attribute| &attribute.value)
}
