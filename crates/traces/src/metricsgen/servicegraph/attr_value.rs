use super::SpanRecord;

pub(crate) fn attr_value<'a>(span: &'a SpanRecord, name: &str) -> Option<&'a str> {
    span.attributes.iter().find_map(|attribute| {
        (attribute.key == name && !attribute.value.is_empty()).then_some(attribute.value.as_str())
    })
}
