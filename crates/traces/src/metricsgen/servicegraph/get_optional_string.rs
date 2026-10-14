use super::{CheckpointCodecError, get_presence};

pub(crate) fn get_optional_string(buf: &mut &[u8]) -> Result<Option<String>, CheckpointCodecError> {
    let present = get_presence(buf)?;
    if !present {
        return Ok(None);
    }
    let value = String::from_utf8(super::super::read_length_prefixed_bytes(buf)?)
        .map_err(|_| CheckpointCodecError::Utf8)?;
    Ok(Some(value))
}
