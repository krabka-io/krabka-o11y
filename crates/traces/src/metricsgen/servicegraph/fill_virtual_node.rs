use super::{ConnectionType, Edge, SpanRecord, attr_value};

pub(crate) fn fill_virtual_node(
    edge: &mut Edge,
    span: &SpanRecord,
    is_client: bool,
    connection_type: ConnectionType,
) {
    if connection_type != ConnectionType::VirtualNode {
        return;
    }
    let Some(peer) = attr_value(span, "peer.service") else {
        return;
    };
    if is_client {
        edge.server_service = Some(peer.to_string());
    } else {
        edge.client_service = Some(peer.to_string());
    }
}
