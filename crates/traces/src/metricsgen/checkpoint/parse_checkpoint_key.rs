use super::{CheckpointCodecError, get_bytes};

///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn parse_checkpoint_key(mut buf: &[u8]) -> Result<ParsedCheckpointKey, CheckpointCodecError> {
    let tenant = String::from_utf8(get_bytes(&mut buf)?).map_err(|_| CheckpointCodecError::Utf8)?;
    let trace_id: [u8; 16] = get_bytes(&mut buf)?
        .try_into()
        .map_err(|_| CheckpointCodecError::BadTraceId)?;
    let edge_id = get_bytes(&mut buf)?;
    Ok(ParsedCheckpointKey {
        tenant,
        trace_id,
        edge_id,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedCheckpointKey {
    pub tenant: String,
    pub trace_id: [u8; 16],
    pub edge_id: Vec<u8>,
}
