use super::*;

pub(crate) fn fixed8(bytes: &[u8], field: &str) -> Result<[u8; 8], WireError> {
    bytes
        .try_into()
        .map_err(|_| WireError::Invalid(format!("{field} must be 8 bytes, got {}", bytes.len())))
}
