use super::*;

pub(crate) fn span_attributes(span: &SpanRef) -> Vec<(String, AttrValue)> {
    span.attributes
        .iter()
        .filter(|(key, _)| !key.starts_with(krabka_traceql::INSTRUMENTATION_ATTR_PREFIX))
        .cloned()
        .collect()
}
