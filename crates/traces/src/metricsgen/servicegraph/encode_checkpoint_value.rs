use super::*;

pub(crate) fn encode_checkpoint_value(edge: &Edge) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_u8(edge.connection_type as u8);
    buf.put_i64(edge.first_seen_ns);
    buf.put_u8(u8::from(edge.failed));
    put_optional_string(&mut buf, edge.client_service.as_deref());
    put_optional_string(&mut buf, edge.server_service.as_deref());
    put_optional_i64(&mut buf, edge.client_latency_ns);
    put_optional_i64(&mut buf, edge.server_latency_ns);
    buf.to_vec()
}
