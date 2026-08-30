use super::*;

pub(crate) fn span_ref(span: &Span, nested: nested_set::NestedSet) -> krabka_traceql::SpanRef {
    krabka_traceql::SpanRef {
        span_id: span.span_id,
        parent_span_id: span.parent_span_id,
        name: span.name.clone(),
        kind: span.kind.as_i32(),
        nested_set_left: nested.left,
        nested_set_right: nested.right,
        nested_set_parent: nested.parent_id,
        start_time_unix_nano: non_negative_u64(span.start_ns),
        duration: Time::from_nanos(span.duration_ns),
        status_code: span.status.as_i32(),
        status_message: span.status_message.clone(),
        instrumentation_name: span.instrumentation_scope.clone(),
        instrumentation_version: span.instrumentation_version.clone(),
        resource_attributes: span
            .resource_attrs
            .iter()
            .filter_map(|attr| traceql_attr(attr).map(|value| (attr.key.clone(), value)))
            .collect(),
        attributes: span
            .span_attrs
            .iter()
            .filter_map(|attr| traceql_attr(attr).map(|value| (attr.key.clone(), value)))
            .collect(),
        events: span
            .events
            .iter()
            .map(|event| event_ref(span, event))
            .collect(),
        links: span.links.iter().map(link_ref).collect(),
    }
}
