use super::*;

pub(crate) fn has_attr(span: &SpanRecord, name: &str) -> bool {
    span.attributes.iter().any(|(key, _)| key == name)
}
