use super::{Buf, CheckpointCodecError, ConnectionType, Edge, get_optional_i64, get_optional_string};

pub(crate) fn decode_checkpoint_value(mut buf: &[u8]) -> Result<Edge, CheckpointCodecError> {
    if buf.len() < 10 {
        return Err(CheckpointCodecError::Truncated);
    }
    let connection_type = match buf.get_u8() {
        0 => ConnectionType::Unset,
        1 => ConnectionType::VirtualNode,
        2 => ConnectionType::MessagingSystem,
        3 => ConnectionType::Database,
        _ => return Err(CheckpointCodecError::BadConnectionType),
    };
    let first_seen_ns = buf.get_i64();
    let failed = buf.get_u8() != 0;
    let client_service = get_optional_string(&mut buf)?;
    let server_service = get_optional_string(&mut buf)?;
    let client_latency_ns = get_optional_i64(&mut buf)?;
    let server_latency_ns = get_optional_i64(&mut buf)?;
    Ok(Edge {
        client_service,
        server_service,
        client_latency_ns,
        server_latency_ns,
        failed,
        connection_type,
        first_seen_ns,
    })
}
