use super::{RESOURCE_ATTR_PREFIX, Span, SpanAttr, push_span_attr};

pub(crate) fn span_attrs(span: &Span) -> Vec<SpanAttr> {
    let mut attrs = Vec::new();
    for attr in &span.resource_attrs {
        push_span_attr(
            &mut attrs,
            format!("{RESOURCE_ATTR_PREFIX}{}", attr.key),
            &attr.value,
        );
    }
    for attr in &span.span_attrs {
        // Reserve the `__resource.` namespace for true resource attributes. A
        // client span attribute keyed under this prefix would otherwise be
        // indistinguishable downstream from a resource-scoped attribute,
        // letting a client spoof `resource.`-scoped values (TraceQL scope
        // bypass / tenant data-integrity). Drop such span attributes.
        if attr.key.starts_with(RESOURCE_ATTR_PREFIX) {
            continue;
        }
        push_span_attr(&mut attrs, attr.key.clone(), &attr.value);
    }
    attrs
}
