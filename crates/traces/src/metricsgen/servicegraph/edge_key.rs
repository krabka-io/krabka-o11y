use super::{SpanKind, SpanRecord};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct EdgeKey {
    pub(crate) trace_id: [u8; 16],
    pub(crate) span_id: [u8; 8],
}

pub(crate) fn edge_key(span: &SpanRecord) -> Option<EdgeKey> {
    match span.kind {
        SpanKind::Client | SpanKind::Producer => Some(EdgeKey {
            trace_id: span.trace_id,
            span_id: span.span_id,
        }),
        SpanKind::Server | SpanKind::Consumer if span.parent_span_id != [0; 8] => Some(EdgeKey {
            trace_id: span.trace_id,
            span_id: span.parent_span_id,
        }),
        _ => None,
    }
}
