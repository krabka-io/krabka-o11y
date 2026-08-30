use super::*;

pub(crate) fn span_links(span: &Span) -> Vec<SpanLink> {
    span.links
        .iter()
        .map(|link| SpanLink {
            linked_trace_id: link.trace_id,
            linked_span_id: link.span_id,
            attrs: event_attrs(&link.attrs),
        })
        .collect()
}
