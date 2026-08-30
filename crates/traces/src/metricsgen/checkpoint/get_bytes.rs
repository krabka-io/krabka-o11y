use super::{Buf, CheckpointCodecError};

pub(crate) fn get_bytes(buf: &mut &[u8]) -> Result<Vec<u8>, CheckpointCodecError> {
    if buf.len() < 4 {
        return Err(CheckpointCodecError::Truncated);
    }
    let len = buf.get_u32() as usize;
    if buf.len() < len {
        return Err(CheckpointCodecError::Truncated);
    }
    let bytes = buf[..len].to_vec();
    buf.advance(len);
    Ok(bytes)
}
