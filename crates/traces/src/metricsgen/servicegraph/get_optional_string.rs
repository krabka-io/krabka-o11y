use super::{Buf, CheckpointCodecError, get_presence};

pub(crate) fn get_optional_string(buf: &mut &[u8]) -> Result<Option<String>, CheckpointCodecError> {
    let present = get_presence(buf)?;
    if !present {
        return Ok(None);
    }
    if buf.len() < 4 {
        return Err(CheckpointCodecError::Truncated);
    }
    let len = buf.get_u32() as usize;
    if buf.len() < len {
        return Err(CheckpointCodecError::Truncated);
    }
    let value = String::from_utf8(buf[..len].to_vec()).map_err(|_| CheckpointCodecError::Utf8)?;
    buf.advance(len);
    Ok(Some(value))
}
