use super::{Span, attr_string, traceql_attr, nested_set, span_ref};

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
        root_trace_name: root.map(|span| span.name.clone()).unwrap_or_default(),
        resource_attributes: root
            .map(|span| {
                span.resource_attrs
                    .iter()
                    .filter_map(|attr| traceql_attr(attr).map(|value| (attr.key.clone(), value)))
                    .collect()
            })
            .unwrap_or_default(),
        spans: spans
            .iter()
            .zip(nested_set::assign_nested_set(spans))
            .map(|(span, nested)| span_ref(span, nested))
            .collect(),
    }
}
