use super::*;

#[must_use]
pub fn encode_checkpoint_key(tenant: &str, trace_id: &[u8; 16], edge_id: &[u8]) -> Bytes {
    let mut buf = BytesMut::new();
    put_bytes(&mut buf, tenant.as_bytes());
    put_bytes(&mut buf, trace_id);
    put_bytes(&mut buf, edge_id);
    buf.freeze()
}
