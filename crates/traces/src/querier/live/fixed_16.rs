use super::{Result, TraceqlError};

pub(crate) fn fixed_16(bytes: &[u8]) -> Result<[u8; 16]> {
    bytes
        .try_into()
        .map_err(|_| TraceqlError::Plan("expected 16-byte trace id".into()))
}
