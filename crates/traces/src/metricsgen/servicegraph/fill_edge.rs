use super::{Edge, SpanRecord};

pub(crate) fn fill_edge(edge: &mut Edge, span: &SpanRecord, is_client: bool, latency_ns: i64) {
    if is_client {
        edge.client_service = Some(span.service_name.clone());
        edge.client_latency_ns = Some(latency_ns);
    } else {
        edge.server_service = Some(span.service_name.clone());
        edge.server_latency_ns = Some(latency_ns);
    }
}
