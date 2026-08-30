use super::*;

pub(crate) fn block_kind(kind: super::super::SpanKind) -> SpanKind {
    match kind {
        super::super::SpanKind::Unspecified => SpanKind::Unspecified,
        super::super::SpanKind::Internal => SpanKind::Internal,
        super::super::SpanKind::Server => SpanKind::Server,
        super::super::SpanKind::Client => SpanKind::Client,
        super::super::SpanKind::Producer => SpanKind::Producer,
        super::super::SpanKind::Consumer => SpanKind::Consumer,
    }
}
