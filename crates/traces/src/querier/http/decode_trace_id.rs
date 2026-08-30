pub(crate) fn decode_trace_id(trace_id: &str) -> Result<[u8; 16], hex::FromHexError> {
    let mut out = [0; 16];
    hex::decode_to_slice(trace_id, &mut out)?;
    Ok(out)
}
