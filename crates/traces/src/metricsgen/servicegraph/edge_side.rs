use super::*;

pub(crate) fn edge_side(kind: SpanKind) -> Option<bool> {
    match kind {
        SpanKind::Client | SpanKind::Producer => Some(true),
        SpanKind::Server | SpanKind::Consumer => Some(false),
        SpanKind::Unspecified | SpanKind::Internal => None,
    }
}
