use super::WireError;

pub(crate) struct TraceIdParts {
    pub(crate) high: i64,
    pub(crate) low: i64,
}

pub(crate) fn trace_id_parts(bytes: &[u8]) -> Result<TraceIdParts, WireError> {
    if bytes.len() != 16 {
        return Err(WireError::Decode("jaeger trace_id must be 16 bytes".into()));
    }
    let high = i64::from_be_bytes(bytes[0..8].try_into().expect("slice length checked"));
    let low = i64::from_be_bytes(bytes[8..16].try_into().expect("slice length checked"));
    Ok(TraceIdParts { high, low })
}
