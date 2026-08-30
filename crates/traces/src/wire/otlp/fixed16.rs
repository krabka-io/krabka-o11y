use super::WireError;

pub(crate) fn fixed16(bytes: &[u8], field: &str) -> Result<[u8; 16], WireError> {
    bytes
        .try_into()
        .map_err(|_| WireError::Invalid(format!("{field} must be 16 bytes, got {}", bytes.len())))
}
