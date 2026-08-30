use super::{SpanRecord, SpanKind};

pub(crate) type EdgeKey = ([u8; 16], [u8; 8]);

pub(crate) fn edge_key(span: &SpanRecord) -> Option<EdgeKey> {
    match span.kind {
        SpanKind::Client | SpanKind::Producer => Some((span.trace_id, span.span_id)),
        SpanKind::Server | SpanKind::Consumer if span.parent_span_id != [0; 8] => {
            Some((span.trace_id, span.parent_span_id))
        }
        _ => None,
    }
}
