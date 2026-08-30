use super::*;

///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn parse_checkpoint_key(
    mut buf: &[u8],
) -> Result<(String, [u8; 16], Vec<u8>), CheckpointCodecError> {
    let tenant = String::from_utf8(get_bytes(&mut buf)?).map_err(|_| CheckpointCodecError::Utf8)?;
    let trace_id: [u8; 16] = get_bytes(&mut buf)?
        .try_into()
        .map_err(|_| CheckpointCodecError::BadTraceId)?;
    let edge_id = get_bytes(&mut buf)?;
    Ok((tenant, trace_id, edge_id))
}
