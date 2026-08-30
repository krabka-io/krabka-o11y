use super::*;

pub(crate) fn attr_value<'a>(span: &'a SpanRecord, name: &str) -> Option<&'a str> {
    span.attributes
        .iter()
        .find_map(|(key, value)| (key == name && !value.is_empty()).then_some(value.as_str()))
}
