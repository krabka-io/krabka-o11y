use super::SpanRef;

pub(crate) fn span_attributes(span: &SpanRef) -> Vec<krabka_traceql::Attribute> {
    span.attributes
        .iter()
        .filter(|attribute| {
            !attribute
                .key
                .starts_with(krabka_traceql::INSTRUMENTATION_ATTR_PREFIX)
        })
        .cloned()
        .collect()
}
