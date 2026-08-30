use super::SpanKind;

pub(crate) fn span_kind_dim(kind: SpanKind) -> &'static str {
    match kind {
        SpanKind::Unspecified => "SPAN_KIND_UNSPECIFIED",
        SpanKind::Internal => "SPAN_KIND_INTERNAL",
        SpanKind::Server => "SPAN_KIND_SERVER",
        SpanKind::Client => "SPAN_KIND_CLIENT",
        SpanKind::Producer => "SPAN_KIND_PRODUCER",
        SpanKind::Consumer => "SPAN_KIND_CONSUMER",
    }
}
