use super::{LinkRecord, traceql_attr};

pub(crate) fn link_ref(link: &LinkRecord) -> krabka_traceql::LinkRef {
    krabka_traceql::LinkRef {
        trace_id: link.trace_id,
        span_id: link.span_id,
        attributes: link
            .attrs
            .iter()
            .filter_map(|attr| traceql_attr(attr).map(|value| (attr.key.clone(), value)))
            .collect(),
    }
}
