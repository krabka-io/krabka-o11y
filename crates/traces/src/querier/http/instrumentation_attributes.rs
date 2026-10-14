use super::{Attribute, SpanRef};

pub(crate) fn instrumentation_attributes(span: &SpanRef) -> Vec<Attribute> {
    span.attributes
        .iter()
        .filter_map(|attribute| {
            attribute
                .key
                .strip_prefix(krabka_traceql::INSTRUMENTATION_ATTR_PREFIX)
                .map(|key| Attribute::new(key, attribute.value.clone()))
        })
        .collect()
}
