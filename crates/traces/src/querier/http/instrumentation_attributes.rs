use super::{SpanRef, AttrValue};

pub(crate) fn instrumentation_attributes(span: &SpanRef) -> Vec<(String, AttrValue)> {
    span.attributes
        .iter()
        .filter_map(|(key, value)| {
            key.strip_prefix(krabka_traceql::INSTRUMENTATION_ATTR_PREFIX)
                .map(|key| (key.to_string(), value.clone()))
        })
        .collect()
}
