use super::WireError;

/// Take `len` bytes from `bytes` at `*pos`, and advance `*pos` past them.
pub(crate) fn take_bytes(bytes: &[u8], pos: &mut usize, len: usize) -> Result<Vec<u8>, WireError> {
    let end = pos
        .checked_add(len)
        .ok_or_else(|| WireError::Decode("binary length overflow".into()))?;
    let out = bytes
        .get(*pos..end)
        .ok_or_else(|| WireError::Decode("truncated binary".into()))?
        .to_vec();
    *pos = end;
    Ok(out)
}
