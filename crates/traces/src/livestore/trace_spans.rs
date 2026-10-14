use super::{Span, attr_string, nested_set, span_ref, traceql_attr};

pub(crate) fn trace_spans(trace_id: &[u8; 16], spans: &[Span]) -> krabka_traceql::TraceSpans {
    let root = spans
        .iter()
        .find(|span| span.is_root())
        .or_else(|| spans.iter().min_by_key(|span| span.start_ns));
    krabka_traceql::TraceSpans {
        trace_id: *trace_id,
        root_service_name: root
            .and_then(|span| attr_string(&span.resource_attrs, "service.name"))
            .unwrap_or_default(),
        root_trace_name: root.map_or_else(Default::default, |span| span.name.clone()),
        resource_attributes: root.map_or_else(Default::default, |span| {
            span.resource_attrs
                .iter()
                .filter_map(|attr| {
                    traceql_attr(attr)
                        .map(|value| krabka_traceql::Attribute::new(attr.key.clone(), value))
                })
                .collect()
        }),
        spans: spans
            .iter()
            .zip(nested_set::assign_nested_set(spans))
            .map(|entry| span_ref(entry.0, entry.1))
            .collect(),
    }
}
