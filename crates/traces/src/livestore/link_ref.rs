use super::{LinkRecord, traceql_attributes};

pub(crate) fn link_ref(link: &LinkRecord) -> krabka_traceql::LinkRef {
    krabka_traceql::LinkRef {
        trace_id: link.trace_id,
        span_id: link.span_id,
        attributes: traceql_attributes(&link.attrs),
    }
}
