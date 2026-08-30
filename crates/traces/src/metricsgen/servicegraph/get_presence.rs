use super::*;

pub(crate) fn get_presence(buf: &mut &[u8]) -> Result<bool, CheckpointCodecError> {
    if buf.is_empty() {
        return Err(CheckpointCodecError::Truncated);
    }
    Ok(buf.get_u8() != 0)
}
