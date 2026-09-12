use super::{BufMut, BytesMut, Edge, put_optional_i64, put_optional_string};

pub(crate) fn encode_checkpoint_value(edge: &Edge) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_u8(edge.connection_type as u8);
    buf.put_i64(edge.first_seen_ns);
    buf.put_u8(u8::from(edge.failed));
    put_optional_string(&mut buf, edge.client_service.as_deref());
    put_optional_string(&mut buf, edge.server_service.as_deref());
    put_optional_i64(&mut buf, edge.client_latency_ns);
    put_optional_i64(&mut buf, edge.server_latency_ns);
    buf.put_f64(edge.multiplier);
    buf.put_u32(edge.labels.len().try_into().unwrap_or(u32::MAX));
    for (name, value) in &edge.labels {
        put_optional_string(&mut buf, Some(name));
        put_optional_string(&mut buf, Some(value));
    }
    buf.to_vec()
}
