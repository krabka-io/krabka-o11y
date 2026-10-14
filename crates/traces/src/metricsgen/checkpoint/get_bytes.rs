use super::CheckpointCodecError;

pub(crate) fn get_bytes(buf: &mut &[u8]) -> Result<Vec<u8>, CheckpointCodecError> {
    super::super::read_length_prefixed_bytes(buf)
}
