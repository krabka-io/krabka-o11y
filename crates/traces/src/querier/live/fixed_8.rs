use super::{Result, TraceqlError};

pub(crate) fn fixed_8(bytes: &[u8]) -> Result<[u8; 8]> {
    bytes
        .try_into()
        .map_err(|_| TraceqlError::Plan("expected 8-byte span id".into()))
}
