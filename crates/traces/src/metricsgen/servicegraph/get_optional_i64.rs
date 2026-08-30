use super::{Buf, CheckpointCodecError, get_presence};

pub(crate) fn get_optional_i64(buf: &mut &[u8]) -> Result<Option<i64>, CheckpointCodecError> {
    let present = get_presence(buf)?;
    if !present {
        return Ok(None);
    }
    if buf.len() < 8 {
        return Err(CheckpointCodecError::Truncated);
    }
    Ok(Some(buf.get_i64()))
}
