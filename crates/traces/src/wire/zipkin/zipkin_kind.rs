use super::*;

pub(crate) fn zipkin_kind(kind: Option<&str>) -> SpanKind {
    match kind {
        Some("SERVER") => SpanKind::Server,
        Some("CLIENT") => SpanKind::Client,
        Some("PRODUCER") => SpanKind::Producer,
        Some("CONSUMER") => SpanKind::Consumer,
        _ => SpanKind::Internal,
    }
}
