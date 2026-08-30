use super::{OtlpLink, otlp_attrs};

pub(crate) fn otlp_link(link: &krabka_traceql::LinkRef) -> OtlpLink {
    OtlpLink {
        trace_id: link.trace_id.to_vec(),
        span_id: link.span_id.to_vec(),
        attributes: otlp_attrs(&link.attributes),
        ..OtlpLink::default()
    }
}
